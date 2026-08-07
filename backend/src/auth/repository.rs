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

pub async fn insert_user(db: &PgPool, id: Uuid, username: &str, password_hash: &str, name: &str, role: &Role) -> Result<(), AppError> {
    sqlx::query("INSERT INTO users (id, username, password_hash, name, role) VALUES ($1, $2, $3, $4, $5)")
        .bind(id)
        .bind(username)
        .bind(password_hash)
        .bind(name)
        .bind(role)
        .execute(db)
        .await
        .db_error("failed to create user")?;
    Ok(())
}

pub async fn update_user(db: &PgPool, id: Uuid, name: &str, role: &Role) -> Result<User, AppError> {
    sqlx::query_as::<_, User>("UPDATE users SET name = $1, role = $2, updated_at = NOW() WHERE id = $3 RETURNING *")
        .bind(name)
        .bind(role)
        .bind(id)
        .fetch_one(db)
        .await
        .db_error("failed to update user")
}

pub async fn delete_user(db: &PgPool, id: Uuid) -> Result<u64, AppError> {
    let result = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(id)
        .execute(db)
        .await
        .db_error("failed to delete user")?;
    Ok(result.rows_affected())
}
