use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use chrono::Utc;
use rand::Rng;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use uuid::Uuid;

use crate::api_keys::models::*;
use crate::auth::middleware::AuthUser;
use crate::errors::{AppError, DbResult};

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn test_generate_api_key_starts_with_sur() {
        let (full, _, _) = generate_api_key();
        assert!(full.starts_with("sur_"));
    }

    #[test]
    fn test_generate_api_key_format() {
        let (full, hash, prefix) = generate_api_key();
        assert_eq!(full.len(), 4 + 40);
        assert_eq!(prefix.len(), 4 + 8);
        assert_eq!(prefix, &full[..12]);
        assert_eq!(hash.len(), 64);
        let expected_hash = hex::encode(Sha256::digest(full.as_bytes()));
        assert_eq!(hash, expected_hash);
    }

    #[test]
    fn test_generate_api_key_unique() {
        let (full1, _, _) = generate_api_key();
        let (full2, _, _) = generate_api_key();
        assert_ne!(full1, full2);
    }
}

fn generate_api_key() -> (String, String, String) {
    let mut rng = rand::thread_rng();
    let random_part: String = (0..40)
        .map(|_| {
            let idx = rng.gen_range(0..62);
            b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"[idx] as char
        })
        .collect();
    let full_key = format!("sur_{}", random_part);
    let prefix = format!("sur_{}", &random_part[..8]);
    let hash = hex::encode(Sha256::digest(full_key.as_bytes()));
    (full_key, hash, prefix)
}

pub async fn list_api_keys(
    Extension(auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
) -> Result<Json<Vec<ApiKeyResponse>>, AppError> {
    let keys = sqlx::query_as::<_, ApiKey>("SELECT * FROM api_keys WHERE user_id = $1 ORDER BY created_at DESC")
        .bind(auth_user.id)
        .fetch_all(&db)
        .await
        .db_error("failed to list API keys")?;

    Ok(Json(keys.into_iter().map(Into::into).collect()))
}

pub async fn create_api_key(
    Extension(auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Json(req): Json<CreateApiKeyRequest>,
) -> Result<(StatusCode, Json<ApiKeyCreatedResponse>), AppError> {
    if req.name.is_empty() {
        return Err(AppError::BadRequest("API key name is required".into()));
    }

    let key_id = Uuid::new_v4();
    let (full_key, key_hash, key_prefix) = generate_api_key();

    sqlx::query("INSERT INTO api_keys (id, user_id, name, key_hash, key_prefix, expires_at) VALUES ($1, $2, $3, $4, $5, $6)")
        .bind(key_id)
        .bind(auth_user.id)
        .bind(&req.name)
        .bind(&key_hash)
        .bind(&key_prefix)
        .bind(req.expires_at)
        .execute(&db)
        .await
        .db_error("failed to create API key")?;

    Ok((
        StatusCode::CREATED,
        Json(ApiKeyCreatedResponse {
            id: key_id,
            name: req.name,
            key: full_key,
            key_prefix,
            is_active: true,
            expires_at: req.expires_at,
            created_at: Utc::now(),
        }),
    ))
}

pub async fn update_api_key(
    Extension(auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(id): Path<Uuid>,
    Json(req): Json<UpdateApiKeyRequest>,
) -> Result<Json<ApiKeyResponse>, AppError> {
    let key = sqlx::query_as::<_, ApiKey>("SELECT * FROM api_keys WHERE id = $1 AND user_id = $2")
        .bind(id)
        .bind(auth_user.id)
        .fetch_optional(&db)
        .await
        .db_error("failed to find API key for update")?
        .ok_or_else(|| AppError::NotFound("API key not found".into()))?;

    let name = req.name.unwrap_or(key.name);
    let is_active = req.is_active.unwrap_or(key.is_active);

    sqlx::query("UPDATE api_keys SET name = $1, is_active = $2 WHERE id = $3")
        .bind(&name)
        .bind(is_active)
        .bind(id)
        .execute(&db)
        .await
        .db_error("failed to update API key")?;

    let updated = sqlx::query_as::<_, ApiKey>("SELECT * FROM api_keys WHERE id = $1")
        .bind(id)
        .fetch_one(&db)
        .await
        .db_error("failed to fetch updated API key")?;

    Ok(Json(updated.into()))
}

pub async fn delete_api_key(
    Extension(auth_user): Extension<AuthUser>,
    State(db): State<PgPool>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let result = sqlx::query("DELETE FROM api_keys WHERE id = $1 AND user_id = $2")
        .bind(id)
        .bind(auth_user.id)
        .execute(&db)
        .await
        .db_error("failed to delete API key")?;

    if result.rows_affected() == 0 {
        return Err(AppError::NotFound("API key not found".into()));
    }

    Ok(StatusCode::NO_CONTENT)
}
