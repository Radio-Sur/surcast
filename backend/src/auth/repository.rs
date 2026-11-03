use sqlx::PgPool;
use uuid::Uuid;

use crate::auth::models::Role;
use crate::auth::models::*;
use crate::errors::{AppError, DbResult};

pub async fn count_users(db: &PgPool) -> Result<i64, AppError> {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM users")
        .fetch_one(db)
        .await
        .db_error("failed to query user count")
}

pub async fn find_user_by_username(db: &PgPool, username: &str) -> Result<Option<User>, AppError> {
    sqlx::query_as::<_, User>("SELECT * FROM users WHERE username = $1")
        .bind(username)
        .fetch_optional(db)
        .await
        .db_error("failed to find user for login")
}

pub async fn find_user_by_id(db: &PgPool, id: Uuid) -> Result<Option<User>, AppError> {
    sqlx::query_as::<_, User>("SELECT * FROM users WHERE id = $1")
        .bind(id)
        .fetch_optional(db)
        .await
        .db_error("failed to find user")
}

pub async fn find_all_users(db: &PgPool) -> Result<Vec<User>, AppError> {
    sqlx::query_as::<_, User>("SELECT * FROM users ORDER BY created_at DESC")
        .fetch_all(db)
        .await
        .db_error("failed to list users")
}

pub async fn insert_user(db: &PgPool, id: Uuid, username: &str, password_hash: &str, role: &Role) -> Result<(), AppError> {
    sqlx::query("INSERT INTO users (id, username, password_hash, role) VALUES ($1, $2, $3, $4)")
        .bind(id)
        .bind(username)
        .bind(password_hash)
        .bind(role)
        .execute(db)
        .await
        .db_error("failed to create user")?;
    Ok(())
}

pub async fn update_user(db: &PgPool, id: Uuid, role: &Role) -> Result<User, AppError> {
    let mut tx = db.begin().await.db_error("begin user update")?;
    protect_last_admin(&mut tx, id, role != &Role::Admin).await?;
    let user = sqlx::query_as::<_, User>("UPDATE users SET role = $1, updated_at = NOW() WHERE id = $2 RETURNING *")
        .bind(role)
        .bind(id)
        .fetch_one(&mut *tx)
        .await
        .db_error("failed to update user")?;
    tx.commit().await.db_error("commit user update")?;
    Ok(user)
}

pub async fn delete_user(db: &PgPool, id: Uuid) -> Result<u64, AppError> {
    let mut tx = db.begin().await.db_error("begin user deletion")?;
    protect_last_admin(&mut tx, id, true).await?;
    let result = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(id)
        .execute(&mut *tx)
        .await
        .db_error("failed to delete user")?;
    tx.commit().await.db_error("commit user deletion")?;
    Ok(result.rows_affected())
}

async fn protect_last_admin(tx: &mut sqlx::PgConnection, id: Uuid, removing_admin: bool) -> Result<(), AppError> {
    sqlx::query("LOCK TABLE users IN EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await
        .db_error("lock user roles")?;
    if removing_admin {
        let last: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM users WHERE id=$1 AND role='admin') AND (SELECT COUNT(*) FROM users WHERE role='admin')=1",
        )
        .bind(id)
        .fetch_one(tx)
        .await
        .db_error("check last administrator")?;
        if last {
            return Err(AppError::conflict("The last administrator cannot be removed or demoted".into()).with_code("LAST_ADMIN"));
        }
    }
    Ok(())
}
