use sqlx::PgPool;
use surcast_backend::auth::models::Role;
use surcast_backend::auth::repository as auth_repo;
use surcast_backend::playlists::repository;
use surcast_backend::songs::repository as songs_repo;
use surcast_backend::songs::repository::InsertSongParams;
use surcast_backend::util::slugify;
use uuid::Uuid;

async fn make_user(db: &PgPool) -> Uuid {
    let id = Uuid::new_v4();
    auth_repo::insert_user(db, id, &format!("user_{id}"), "hash", "Playlist Tester", &Role::Admin)
        .await
        .unwrap();
    id
}

async fn make_song(db: &PgPool, user_id: Uuid) -> Uuid {
    let id = Uuid::new_v4();
    songs_repo::insert_song_record(
        db,
        &InsertSongParams {
            id,
            title: "Playlist Song".into(),
            artist: "Artist".into(),
            album: "Album".into(),
            cover_path: "".into(),
            file_path: "/tmp/playlist.mp3".into(),
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

#[sqlx::test(migrations = "./migrations")]
async fn test_insert_and_find_playlist(db: PgPool) {
    let user_id = make_user(&db).await;
    let id = Uuid::new_v4();
    let slug = slugify("My Playlist");

    repository::insert_playlist(&db, id, "My Playlist", "My description", &slug, user_id)
        .await
        .expect("insert failed");

    let pl = repository::find_playlist_by_id(&db, id)
        .await
        .expect("find failed")
        .expect("not found");

    assert_eq!(pl.name, "My Playlist");
    assert_eq!(pl.description, "My description");
    assert_eq!(pl.created_by, user_id);
    assert_eq!(pl.slug, Some("my-playlist".to_string()));
}
#[ignore]
#[sqlx::test(migrations = "./migrations")]
async fn test_find_all_playlists(db: PgPool) {
    let user_id = make_user(&db).await;

    repository::insert_playlist(&db, Uuid::new_v4(), "P1", "", "p1", user_id)
        .await
        .unwrap();
    repository::insert_playlist(&db, Uuid::new_v4(), "P2", "", "p2", user_id)
        .await
        .unwrap();

    let playlists = repository::find_all_playlists(&db).await.expect("find_all failed");
    assert!(playlists.len() >= 2);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_update_playlist(db: PgPool) {
    let user_id = make_user(&db).await;
    let id = Uuid::new_v4();

    repository::insert_playlist(&db, id, "Old", "Old desc", "old", user_id)
        .await
        .unwrap();
    repository::update_playlist_fields(&db, id, "New", "New desc", "new")
        .await
        .expect("update failed");

    let pl = repository::find_playlist_by_id(&db, id)
        .await
        .expect("find failed")
        .expect("not found");
    assert_eq!(pl.name, "New");
    assert_eq!(pl.description, "New desc");
    assert_eq!(pl.slug, Some("new".to_string()));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_delete_playlist(db: PgPool) {
    let user_id = make_user(&db).await;
    let id = Uuid::new_v4();

    repository::insert_playlist(&db, id, "Delete Me", "", "delete-me", user_id)
        .await
        .unwrap();

    let deleted = repository::delete_playlist(&db, id).await.expect("delete failed");
    assert_eq!(deleted, 1);

    let pl = repository::find_playlist_by_id(&db, id).await.unwrap();
    assert!(pl.is_none());
}
#[ignore]
#[sqlx::test(migrations = "./migrations")]
async fn test_playlist_exists(db: PgPool) {
    let user_id = make_user(&db).await;
    let id = Uuid::new_v4();

    assert!(!repository::playlist_exists(&db, id).await.unwrap());

    repository::insert_playlist(&db, id, "Exists", "", "exists", user_id).await.unwrap();

    assert!(repository::playlist_exists(&db, id).await.unwrap());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_playlist_song_stats(db: PgPool) {
    let mut conn = db.acquire().await.unwrap();
    let user_id = make_user(&db).await;
    let playlist_id = Uuid::new_v4();
    let song_id = make_song(&db, user_id).await;

    repository::insert_playlist(&db, playlist_id, "Stats", "", "stats", user_id)
        .await
        .unwrap();

    let (count, duration) = repository::playlist_song_stats(&db, playlist_id).await.expect("stats failed");
    assert_eq!(count, 0);
    assert_eq!(duration, 0);

    let position = repository::compute_max_playlist_position(&mut conn, playlist_id)
        .await
        .expect("max position failed");
    assert_eq!(position, 0);

    repository::insert_playlist_song(&mut conn, playlist_id, song_id, position)
        .await
        .expect("insert song failed");

    let (count, duration) = repository::playlist_song_stats(&db, playlist_id).await.expect("stats failed");
    assert_eq!(count, 1);
    assert_eq!(duration, 180);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_insert_and_delete_playlist_song(db: PgPool) {
    let mut conn = db.acquire().await.unwrap();
    let user_id = make_user(&db).await;
    let playlist_id = Uuid::new_v4();
    let song_id = make_song(&db, user_id).await;

    repository::insert_playlist(&db, playlist_id, "Songs", "", "songs", user_id)
        .await
        .unwrap();

    let pos = repository::compute_max_playlist_position(&mut conn, playlist_id).await.unwrap();
    repository::insert_playlist_song(&mut conn, playlist_id, song_id, pos)
        .await
        .expect("insert song failed");

    let ids = repository::find_playlist_song_ids(&db, playlist_id).await.expect("find ids failed");
    assert_eq!(ids, vec![song_id]);

    repository::delete_playlist_song(&mut conn, playlist_id, song_id)
        .await
        .expect("delete song failed");

    let ids = repository::find_playlist_song_ids(&db, playlist_id).await.expect("find ids failed");
    assert!(ids.is_empty());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_find_playlist_song_ids(db: PgPool) {
    let mut conn = db.acquire().await.unwrap();
    let user_id = make_user(&db).await;
    let playlist_id = Uuid::new_v4();
    let song_a = make_song(&db, user_id).await;
    let song_b = make_song(&db, user_id).await;

    repository::insert_playlist(&db, playlist_id, "Multi", "", "multi", user_id)
        .await
        .unwrap();

    let pos1 = repository::compute_max_playlist_position(&mut conn, playlist_id).await.unwrap();
    repository::insert_playlist_song(&mut conn, playlist_id, song_a, pos1)
        .await
        .unwrap();
    let pos2 = repository::compute_max_playlist_position(&mut conn, playlist_id).await.unwrap();
    repository::insert_playlist_song(&mut conn, playlist_id, song_b, pos2)
        .await
        .unwrap();

    let ids = repository::find_playlist_song_ids(&db, playlist_id).await.expect("find ids failed");
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&song_a));
    assert!(ids.contains(&song_b));
}
#[ignore]
#[sqlx::test(migrations = "./migrations")]
async fn test_resolve_playlist_id(db: PgPool) {
    let user_id = make_user(&db).await;
    let id = Uuid::new_v4();

    repository::insert_playlist(&db, id, "Resolve Test", "", "resolve-test", user_id)
        .await
        .unwrap();

    let by_uuid = repository::resolve_playlist_id(&db, &id.to_string()).await.unwrap();
    assert_eq!(by_uuid, id);

    let by_slug = repository::resolve_playlist_id(&db, "resolve-test").await.unwrap();
    assert_eq!(by_slug, id);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_find_playlist_by_slug(db: PgPool) {
    let user_id = make_user(&db).await;
    let id = Uuid::new_v4();

    repository::insert_playlist(&db, id, "Slug Test", "", "slug-test", user_id)
        .await
        .unwrap();

    let pl = repository::find_playlist_by_slug(&db, "slug-test")
        .await
        .unwrap()
        .expect("not found");
    assert_eq!(pl.id, id);
    assert_eq!(pl.name, "Slug Test");
}
