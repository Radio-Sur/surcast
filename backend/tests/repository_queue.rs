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
