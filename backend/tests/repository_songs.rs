use sqlx::PgPool;
use surcast_backend::auth::models::Role;
use surcast_backend::auth::repository as auth_repo;
use surcast_backend::songs::repository;
use surcast_backend::songs::repository::InsertSongParams;
use surcast_backend::stations::repository as stations_repo;
use surcast_backend::stations::repository::CreateStationParams;
use uuid::Uuid;

async fn make_user(db: &PgPool) -> Uuid {
    let id = Uuid::new_v4();
    auth_repo::insert_user(db, id, &format!("user_{id}"), "hash", "Song Tester", &Role::Admin)
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
            name: "Song Station".into(),
            description: "".into(),
            slug: format!("song-st-{id}"),
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

fn song_params(id: Uuid, user_id: Uuid) -> InsertSongParams {
    InsertSongParams {
        id,
        title: "Test Song".into(),
        artist: "Test Artist".into(),
        album: "Test Album".into(),
        cover_path: "covers/test.jpg".into(),
        file_path: "/tmp/test_song.mp3".into(),
        file_size: 2048,
        mime_type: "audio/mpeg".into(),
        duration: 240,
        uploaded_by: user_id,
        ..InsertSongParams::default()
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn test_insert_and_find_song(db: PgPool) {
    let user_id = make_user(&db).await;
    let id = Uuid::new_v4();

    repository::insert_song_record(&db, &song_params(id, user_id))
        .await
        .expect("insert failed");

    let song = repository::find_song_by_id(&db, id).await.expect("find failed").expect("not found");

    assert_eq!(song.title, "Test Song");
    assert_eq!(song.artist, "Test Artist");
    assert_eq!(song.duration, 240);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_find_all_songs(db: PgPool) {
    let user_id = make_user(&db).await;

    repository::insert_song_record(&db, &song_params(Uuid::new_v4(), user_id))
        .await
        .unwrap();
    repository::insert_song_record(&db, &song_params(Uuid::new_v4(), user_id))
        .await
        .unwrap();

    let songs = repository::find_all_songs(&db).await.expect("find_all failed");
    assert!(songs.len() >= 2);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_update_song(db: PgPool) {
    let user_id = make_user(&db).await;
    let id = Uuid::new_v4();

    repository::insert_song_record(&db, &song_params(id, user_id)).await.unwrap();

    repository::update_song_fields(&db, id, "Updated Title", "Updated Artist", "Updated Album", 300)
        .await
        .expect("update failed");

    let song = repository::find_song_by_id(&db, id).await.expect("find failed").expect("not found");
    assert_eq!(song.title, "Updated Title");
    assert_eq!(song.artist, "Updated Artist");
    assert_eq!(song.duration, 300);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_delete_song(db: PgPool) {
    let user_id = make_user(&db).await;
    let id = Uuid::new_v4();

    repository::insert_song_record(&db, &song_params(id, user_id)).await.unwrap();
    repository::delete_song(&db, id).await.expect("delete failed");

    let song = repository::find_song_by_id(&db, id).await.unwrap();
    assert!(song.is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_assign_song_to_station_and_find_station_ids(db: PgPool) {
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    let song_id = Uuid::new_v4();

    repository::insert_song_record(&db, &song_params(song_id, user_id)).await.unwrap();

    repository::assign_song_to_station(&db, station_id, song_id)
        .await
        .expect("assign failed");

    let station_ids = repository::find_station_ids_for_song(&db, song_id)
        .await
        .expect("find station ids failed");
    assert!(station_ids.iter().any(|(sid,)| *sid == station_id));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_delete_song_globally_clears_station_library(db: PgPool) {
    let user_id = make_user(&db).await;
    let station_id = make_station(&db, user_id).await;
    let song_id = Uuid::new_v4();

    repository::insert_song_record(&db, &song_params(song_id, user_id)).await.unwrap();
    repository::assign_song_to_station(&db, station_id, song_id).await.unwrap();

    let station_ids = repository::find_station_ids_for_song(&db, song_id).await.unwrap();
    assert!(!station_ids.is_empty());

    repository::delete_song_globally(&db, song_id).await.expect("global delete failed");

    let station_ids = repository::find_station_ids_for_song(&db, song_id).await.unwrap();
    assert!(station_ids.is_empty());
}
