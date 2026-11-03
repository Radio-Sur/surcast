use sqlx::PgPool;
use surcast_backend::auth::{models::Role, repository};
use uuid::Uuid;

#[sqlx::test(migrations = "./migrations")]
async fn concurrent_admin_removal_keeps_one_admin(db: PgPool) {
    let a = Uuid::new_v4();
    let b = Uuid::new_v4();
    repository::insert_user(&db, a, "first", "unused", &Role::Admin).await.unwrap();
    repository::insert_user(&db, b, "second", "unused", &Role::Admin).await.unwrap();
    let (deleted, demoted) = tokio::join!(repository::delete_user(&db, a), repository::update_user(&db, b, &Role::Demo));
    assert_ne!(deleted.is_ok(), demoted.is_ok());
    let admins: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE role='admin'")
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(admins, 1);
}
