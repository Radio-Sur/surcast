use sqlx::PgPool;
use surcast_backend::auth::models::Role;
use surcast_backend::auth::repository;
use uuid::Uuid;

#[sqlx::test(migrations = "./migrations")]
async fn test_insert_and_find_user(db: PgPool) {
    let id = Uuid::new_v4();
    repository::insert_user(&db, id, "testuser", "hash", "Test User", &Role::Viewer)
        .await
        .expect("insert failed");

    let user = repository::find_user_by_username(&db, "testuser")
        .await
        .expect("find failed")
        .expect("not found");

    assert_eq!(user.username, "testuser");
    assert_eq!(user.name, "Test User");
    assert_eq!(user.role, Role::Viewer);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_find_user_by_id(db: PgPool) {
    let id = Uuid::new_v4();
    repository::insert_user(&db, id, "byid", "hash", "By ID", &Role::Admin)
        .await
        .expect("insert failed");

    let user = repository::find_user_by_id(&db, id).await.expect("find failed").expect("not found");

    assert_eq!(user.id, id);
    assert_eq!(user.role, Role::Admin);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_find_all_users(db: PgPool) {
    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    repository::insert_user(&db, id1, "user_a", "hash", "User A", &Role::Manager)
        .await
        .unwrap();
    repository::insert_user(&db, id2, "user_b", "hash", "User B", &Role::Viewer)
        .await
        .unwrap();

    let users = repository::find_all_users(&db).await.expect("find_all failed");
    assert!(users.len() >= 2);
    let names: Vec<&str> = users.iter().map(|u| u.name.as_str()).collect();
    assert!(names.contains(&"User A"));
    assert!(names.contains(&"User B"));
}

#[sqlx::test(migrations = "./migrations")]
async fn test_update_user(db: PgPool) {
    let id = Uuid::new_v4();
    repository::insert_user(&db, id, "update_me", "hash", "Old Name", &Role::Viewer)
        .await
        .unwrap();

    repository::update_user(&db, id, "New Name", &Role::Admin)
        .await
        .expect("update failed");

    let user = repository::find_user_by_id(&db, id).await.expect("find failed").expect("not found");

    assert_eq!(user.name, "New Name");
    assert_eq!(user.role, Role::Admin);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_delete_user(db: PgPool) {
    let id = Uuid::new_v4();
    repository::insert_user(&db, id, "delete_me", "hash", "Delete Me", &Role::Viewer)
        .await
        .unwrap();

    let deleted = repository::delete_user(&db, id).await.expect("delete failed");
    assert_eq!(deleted, 1);

    let user = repository::find_user_by_username(&db, "delete_me").await.expect("find failed");
    assert!(user.is_none());
}

#[sqlx::test(migrations = "./migrations")]
async fn test_is_setup_complete(db: PgPool) {
    let count = repository::count_users(&db).await.expect("count failed");
    assert_eq!(count, 0);

    repository::insert_user(&db, Uuid::new_v4(), "setup_user", "hash", "Setup", &Role::Admin)
        .await
        .unwrap();

    let count = repository::count_users(&db).await.expect("count failed");
    assert!(count > 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_count_users(db: PgPool) {
    let before = repository::count_users(&db).await.expect("count failed");
    repository::insert_user(
        &db,
        Uuid::new_v4(),
        &format!("count1_{}", Uuid::new_v4()),
        "hash",
        "Count1",
        &Role::Viewer,
    )
    .await
    .unwrap();
    repository::insert_user(
        &db,
        Uuid::new_v4(),
        &format!("count2_{}", Uuid::new_v4()),
        "hash",
        "Count2",
        &Role::Manager,
    )
    .await
    .unwrap();

    let after = repository::count_users(&db).await.expect("count failed");
    assert_eq!(after, before + 2);
}
