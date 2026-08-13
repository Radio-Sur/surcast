mod common;

use surcast_backend::auth::models::Role;
use surcast_backend::auth::repository as auth_repo;
use surcast_backend::playlists::repository as playlists_repo;
use surcast_backend::songs::repository as songs_repo;
use surcast_backend::songs::repository::InsertSongParams;
use surcast_backend::stations::queue_repo;
use surcast_backend::stations::repository as stations_repo;
use surcast_backend::stations::repository::CreateStationParams;
use uuid::Uuid;

async fn make_user(db: &sqlx::PgPool) -> Uuid {
    let id = Uuid::new_v4();
    auth_repo::insert_user(db, id, &format!("user_{id}"), "hash", "Queue Tester", &Role::Admin)
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
            name: "Queue Station".into(),
            description: "".into(),
            slug: format!("queue-{id}"),
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

async fn make_song(db: &sqlx::PgPool, user_id: Uuid) -> Uuid {
    let id = Uuid::new_v4();
    songs_repo::insert_song_record(
        db,
        &InsertSongParams {
            id,
            title: "Queue Song".into(),
            artist: "Artist".into(),
            album: "Album".into(),
            cover_path: "".into(),
            file_path: "/tmp/queue.mp3".into(),
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

#[tokio::test]
async fn test_insert_queue_item_and_find_all() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    let song_id = make_song(&db, user_id).await;

    queue_repo::insert_queue_item(&db, station_id, song_id, 0, None)
        .await
        .expect("insert queue item failed");

    let items = queue_repo::find_queue_items_all(&db, station_id)
        .await
        .expect("find queue items failed");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].2, song_id);
    assert_eq!(items[0].3, 0);
}

#[tokio::test]
async fn test_queue_next_position() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    let song_id = make_song(&db, user_id).await;

    let pos = queue_repo::queue_next_position(&db, station_id)
        .await
        .expect("next position failed");
    assert_eq!(pos, 0);

    queue_repo::insert_queue_item(&db, station_id, song_id, pos, None).await.unwrap();

    let pos = queue_repo::queue_next_position(&db, station_id)
        .await
        .expect("next position failed");
    assert_eq!(pos, 1);
}

#[tokio::test]
async fn test_reorder_queue() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    let song1 = make_song(&db, user_id).await;
    let song2 = make_song(&db, user_id).await;

    queue_repo::insert_queue_item(&db, station_id, song1, 0, None).await.unwrap();
    queue_repo::insert_queue_item(&db, station_id, song2, 1, None).await.unwrap();

    let items = queue_repo::find_queue_items_all(&db, station_id).await.expect("find failed");
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].3, 0);
    assert_eq!(items[1].3, 1);

    queue_repo::set_queue_position(&db, items[1].0, 0).await.unwrap();
    queue_repo::set_queue_position(&db, items[0].0, 1).await.unwrap();

    let items = queue_repo::find_queue_items_all(&db, station_id).await.expect("find failed");
    let positions: Vec<i32> = items.iter().map(|i| i.3).collect();
    assert_eq!(positions, vec![0, 1]);
}

#[tokio::test]
async fn test_delete_queue_by_id() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    let song_id = make_song(&db, user_id).await;

    queue_repo::insert_queue_item(&db, station_id, song_id, 0, None).await.unwrap();

    let items = queue_repo::find_queue_items_all(&db, station_id).await.expect("find failed");
    assert_eq!(items.len(), 1);

    queue_repo::delete_queue_by_id(&db, items[0].0, station_id)
        .await
        .expect("delete failed");

    let items = queue_repo::find_queue_items_all(&db, station_id).await.expect("find failed");
    assert!(items.is_empty());
}

#[tokio::test]
async fn test_shift_queue_positions_from() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    let song1 = make_song(&db, user_id).await;
    let song2 = make_song(&db, user_id).await;

    queue_repo::insert_queue_item(&db, station_id, song1, 0, None).await.unwrap();

    queue_repo::shift_queue_positions_from(&db, station_id, 0)
        .await
        .expect("shift failed");

    queue_repo::insert_queue_item(&db, station_id, song2, 0, None).await.unwrap();

    let items = queue_repo::find_queue_items_all(&db, station_id).await.expect("find failed");
    assert_eq!(items.len(), 2);
    let positions: Vec<i32> = items.iter().map(|i| i.3).collect();
    assert!(positions.contains(&0));
    assert!(positions.contains(&1));
}

#[tokio::test]
async fn test_delete_queue_by_playlist() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    let song_id = make_song(&db, user_id).await;
    let playlist_id = Uuid::new_v4();
    playlists_repo::insert_playlist(&db, playlist_id, "Test Playlist", "desc", "test-playlist", user_id)
        .await
        .unwrap();

    queue_repo::insert_queue_item(&db, station_id, song_id, 0, Some(playlist_id))
        .await
        .unwrap();

    let items = queue_repo::find_queue_items_all(&db, station_id).await.expect("find failed");
    assert_eq!(items.len(), 1);

    queue_repo::delete_queue_by_playlist(&db, station_id, playlist_id)
        .await
        .expect("delete by playlist failed");

    let items = queue_repo::find_queue_items_all(&db, station_id).await.expect("find failed");
    assert!(items.is_empty());
}

#[tokio::test]
async fn test_trim_consumed_queue_items_removes_played_rows() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    let stale_song = make_song(&db, user_id).await;
    let current_song = make_song(&db, user_id).await;

    // stale played rows at positions 0 and 1, current track at position 2
    queue_repo::insert_queue_item(&db, station_id, stale_song, 0, None).await.unwrap();
    queue_repo::insert_queue_item(&db, station_id, stale_song, 1, None).await.unwrap();
    queue_repo::insert_queue_item(&db, station_id, current_song, 2, None).await.unwrap();

    let stale_ids: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM station_queue WHERE station_id = $1 AND position < 2 ORDER BY position")
        .bind(station_id)
        .fetch_all(&db)
        .await
        .unwrap();
    assert_eq!(stale_ids.len(), 2);
    let current_item: Uuid = sqlx::query_scalar("SELECT id FROM station_queue WHERE station_id = $1 AND position = 2")
        .bind(station_id)
        .fetch_one(&db)
        .await
        .unwrap();

    // durable format-1 cursor: stale rows are tracked by identity
    sqlx::query(
        "UPDATE stations SET current_queue_item_id = $1, consumed_queue_item_ids = $2, \
         current_song_index = 2, current_queue_cursor_format = 1 WHERE id = $3",
    )
    .bind(current_item)
    .bind(&stale_ids)
    .bind(station_id)
    .execute(&db)
    .await
    .unwrap();

    queue_repo::trim_consumed_queue_items(&db, station_id).await.unwrap();

    let remaining: Vec<(Uuid, i32)> = sqlx::query_as("SELECT id, position FROM station_queue WHERE station_id = $1 ORDER BY position")
        .bind(station_id)
        .fetch_all(&db)
        .await
        .unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].0, current_item);
    assert_eq!(remaining[0].1, 2);
}

#[tokio::test]
async fn test_trim_consumed_queue_items_ignores_stale_position_cutoff() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    let played_song = make_song(&db, user_id).await;
    let current_song = make_song(&db, user_id).await;
    let upcoming_a = make_song(&db, user_id).await;
    let upcoming_b = make_song(&db, user_id).await;

    // Regression: a reorder renumbered the queue densely (current at 0,
    // upcoming at 1..) while stations.current_song_index was left at its old
    // value (10). The old positional clause `position < current_song_index`
    // then deleted every current/upcoming row on the next enqueue.
    queue_repo::insert_queue_item(&db, station_id, played_song, 0, None).await.unwrap();
    queue_repo::insert_queue_item(&db, station_id, current_song, 1, None).await.unwrap();
    queue_repo::insert_queue_item(&db, station_id, upcoming_a, 2, None).await.unwrap();
    queue_repo::insert_queue_item(&db, station_id, upcoming_b, 3, None).await.unwrap();

    let played_id: Uuid = sqlx::query_scalar("SELECT id FROM station_queue WHERE station_id = $1 AND position = 0")
        .bind(station_id)
        .fetch_one(&db)
        .await
        .unwrap();
    let current_id: Uuid = sqlx::query_scalar("SELECT id FROM station_queue WHERE station_id = $1 AND position = 1")
        .bind(station_id)
        .fetch_one(&db)
        .await
        .unwrap();

    sqlx::query(
        "UPDATE stations SET current_queue_item_id = $1, consumed_queue_item_ids = $2, \
         current_song_index = 10, current_queue_cursor_format = 1 WHERE id = $3",
    )
    .bind(current_id)
    .bind(&vec![played_id])
    .bind(station_id)
    .execute(&db)
    .await
    .unwrap();

    queue_repo::trim_consumed_queue_items(&db, station_id).await.unwrap();

    // Only the identity-consumed row is gone; current and upcoming survive.
    let remaining: Vec<(Uuid, i32)> = sqlx::query_as("SELECT id, position FROM station_queue WHERE station_id = $1 ORDER BY position")
        .bind(station_id)
        .fetch_all(&db)
        .await
        .unwrap();
    assert_eq!(remaining.len(), 3);
    assert_eq!(remaining[0].1, 1);
    assert_eq!(remaining[1].1, 2);
    assert_eq!(remaining[2].1, 3);
}

#[tokio::test]
async fn test_renumber_syncs_current_song_index() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    let song_a = make_song(&db, user_id).await;
    let song_b = make_song(&db, user_id).await;
    let song_c = make_song(&db, user_id).await;

    queue_repo::insert_queue_item(&db, station_id, song_a, 5, None).await.unwrap();
    queue_repo::insert_queue_item(&db, station_id, song_b, 6, None).await.unwrap();
    queue_repo::insert_queue_item(&db, station_id, song_c, 7, None).await.unwrap();

    let current_id: Uuid = sqlx::query_scalar("SELECT id FROM station_queue WHERE station_id = $1 AND position = 6")
        .bind(station_id)
        .fetch_one(&db)
        .await
        .unwrap();

    sqlx::query(
        "UPDATE stations SET current_queue_item_id = $1, consumed_queue_item_ids = $2, \
         current_song_index = 6, current_queue_cursor_format = 1 WHERE id = $3",
    )
    .bind(current_id)
    .bind(&Vec::<Uuid>::new())
    .bind(station_id)
    .execute(&db)
    .await
    .unwrap();

    // reorder: renumber densely, B (current) moves to index 0
    let ids: Vec<(Uuid,)> = sqlx::query_as("SELECT id FROM station_queue WHERE station_id = $1 ORDER BY id")
        .bind(station_id)
        .fetch_all(&db)
        .await
        .unwrap();
    for (i, (id,)) in ids.iter().enumerate() {
        queue_repo::set_queue_position(&db, *id, i as i32).await.unwrap();
    }
    queue_repo::sync_current_song_index_after_renumber(&db, station_id).await.unwrap();

    let indexed: i32 = sqlx::query_scalar("SELECT current_song_index FROM stations WHERE id = $1")
        .bind(station_id)
        .fetch_one(&db)
        .await
        .unwrap();
    let current_pos: i32 = sqlx::query_scalar("SELECT position FROM station_queue WHERE id = $1")
        .bind(current_id)
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(current_pos, 0);
    assert_eq!(indexed, 0);

    // insert-at-position before the current shifts it up by one; the index
    // must follow (the handler calls the same sync after the shift)
    queue_repo::shift_queue_positions_from(&db, station_id, 0).await.unwrap();
    queue_repo::insert_queue_item_at(&db, station_id, song_a, 0).await.unwrap();
    queue_repo::sync_current_song_index_after_renumber(&db, station_id).await.unwrap();

    let indexed: i32 = sqlx::query_scalar("SELECT current_song_index FROM stations WHERE id = $1")
        .bind(station_id)
        .fetch_one(&db)
        .await
        .unwrap();
    let current_pos: i32 = sqlx::query_scalar("SELECT position FROM station_queue WHERE id = $1")
        .bind(current_id)
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(current_pos, 1);
    assert_eq!(indexed, 1);
}

#[tokio::test]
async fn test_trim_consumed_queue_items_keeps_unplayed_rows() {
    let db = common::setup_db().await;
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    let song = make_song(&db, user_id).await;

    // nothing consumed: nothing may be deleted
    queue_repo::insert_queue_item(&db, station_id, song, 0, None).await.unwrap();
    queue_repo::insert_queue_item(&db, station_id, song, 1, None).await.unwrap();

    queue_repo::trim_consumed_queue_items(&db, station_id).await.unwrap();

    let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM station_queue WHERE station_id = $1")
        .bind(station_id)
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(remaining, 2);
}
