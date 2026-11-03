use axum::extract::State;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use bcrypt::{hash, verify, DEFAULT_COST};
use chrono::Utc;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use sqlx::PgPool;
use uuid::Uuid;

use crate::auth::middleware::AuthUser;
use crate::auth::models::*;
use crate::auth::repository;
use crate::config::Config;
use crate::errors::{AppError, DbResult};

pub async fn setup_status(State(db): State<PgPool>) -> Result<Json<SetupStatus>, AppError> {
    let count = repository::count_users(&db).await?;
    Ok(Json(SetupStatus { setup_complete: count > 0 }))
}

pub async fn setup_init(
    State(db): State<PgPool>,
    Json(req): Json<SetupRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), AppError> {
    validate_credentials(&req.username, &req.password)?;
    if repository::count_users(&db).await? > 0 {
        return Err(AppError::bad_request("Setup already completed".into()).with_code("SETUP_ALREADY_COMPLETE"));
    }
    let password_hash = hash_password(req.password).await?;
    let user_id = Uuid::new_v4();
    let mut tx = db.begin().await.db_error("begin initial setup")?;
    sqlx::query("LOCK TABLE users IN EXCLUSIVE MODE")
        .execute(&mut *tx)
        .await
        .db_error("lock initial setup")?;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&mut *tx)
        .await
        .db_error("check initial setup")?;
    if count > 0 {
        return Err(AppError::bad_request("Setup already completed".into()).with_code("SETUP_ALREADY_COMPLETE"));
    }
    sqlx::query("INSERT INTO users(id,username,password_hash,role) VALUES($1,$2,$3,'admin')")
        .bind(user_id)
        .bind(req.username.trim())
        .bind(password_hash)
        .execute(&mut *tx)
        .await
        .db_error("create initial admin")?;
    tx.commit().await.db_error("commit initial setup")?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({ "message": "Admin account created. Please sign in." })),
    ))
}

pub async fn login(
    State(db): State<PgPool>,
    State(config): State<Config>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<AuthResponse>, AppError> {
    let user = repository::find_user_by_username(&db, &req.username)
        .await?
        .ok_or_else(|| AppError::unauthorized("Invalid username or password".into()).with_code("AUTH_INVALID_CREDENTIALS"))?;

    let password_hash = user.password_hash.clone();
    let valid = tokio::task::spawn_blocking(move || verify(req.password, &password_hash))
        .await
        .map_err(|_| AppError::internal("Password verification task failed".into()))?
        .db_error("failed to verify password")?;

    if !valid {
        return Err(AppError::unauthorized("Invalid username or password".into()).with_code("AUTH_INVALID_CREDENTIALS"));
    }

    if user.role == Role::Demo && !crate::demo::ready(&db, user.id).await? {
        return Err(AppError::forbidden("Demo sandbox is not yet configured".into()).with_code("DEMO_NOT_READY"));
    }

    let (access_token, refresh_token) = generate_tokens(&user, &config)?;

    Ok(Json(AuthResponse {
        access_token,
        refresh_token,
        user: user.into(),
    }))
}

pub async fn refresh(
    State(db): State<PgPool>,
    State(config): State<Config>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<AuthResponse>, AppError> {
    let refresh_token = body
        .get("refresh_token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| AppError::bad_request("refresh_token required".into()).with_code("AUTH_REFRESH_REQUIRED"))?;

    let token_data = decode::<Claims>(
        refresh_token,
        &DecodingKey::from_secret(config.jwt_secret.as_bytes()),
        &Validation::default(),
    )
    .map_err(|_| AppError::unauthorized("Invalid refresh token".into()).with_code("AUTH_REFRESH_INVALID"))?;
    if token_data.claims.purpose != TokenPurpose::Refresh {
        return Err(AppError::unauthorized("Refresh token required".into()).with_code("AUTH_REFRESH_INVALID"));
    }

    let user_id = Uuid::parse_str(&token_data.claims.sub).map_err(|_| AppError::unauthorized("Invalid token".into()))?;

    let user = repository::find_user_by_id(&db, user_id)
        .await?
        .ok_or_else(|| AppError::unauthorized("User not found".into()))?;

    if user.role == Role::Demo && !crate::demo::ready(&db, user.id).await? {
        return Err(AppError::forbidden("Demo sandbox is not yet configured".into()).with_code("DEMO_NOT_READY"));
    }

    let (access_token, new_refresh_token) = generate_tokens(&user, &config)?;

    Ok(Json(AuthResponse {
        access_token,
        refresh_token: new_refresh_token,
        user: user.into(),
    }))
}

pub async fn me(Extension(auth_user): Extension<AuthUser>, State(db): State<PgPool>) -> Result<Json<UserResponse>, AppError> {
    let user = repository::find_user_by_id(&db, auth_user.id)
        .await?
        .ok_or_else(|| AppError::not_found("User not found".into()))?;

    Ok(Json(user.into()))
}

pub async fn list_users(State(db): State<PgPool>) -> Result<Json<Vec<UserResponse>>, AppError> {
    let users = repository::find_all_users(&db).await?;

    Ok(Json(users.into_iter().map(|u| u.into()).collect()))
}

pub async fn create_user(
    State(db): State<PgPool>,
    Json(req): Json<CreateUserRequest>,
) -> Result<(StatusCode, Json<UserResponse>), AppError> {
    let username = req.username.trim();
    validate_credentials(username, &req.password)?;
    let role = req
        .role
        .parse::<Role>()
        .map_err(|_| AppError::bad_request("Role must be administrator or demo".into()).with_code("ROLE_INVALID"))?;
    let password_hash = hash_password(req.password).await?;
    let id = Uuid::new_v4();
    let result = sqlx::query("INSERT INTO users (id, username, password_hash, role) VALUES ($1,$2,$3,$4)")
        .bind(id)
        .bind(username)
        .bind(password_hash)
        .bind(role)
        .execute(&db)
        .await;
    if let Err(sqlx::Error::Database(ref error)) = result {
        if error.is_unique_violation() {
            return Err(AppError::conflict("Username already exists".into()).with_code("USERNAME_TAKEN"));
        }
    }
    result.db_error("failed to create user")?;
    let user = repository::find_user_by_id(&db, id)
        .await?
        .ok_or_else(|| AppError::internal("Created user not found".into()))?;
    Ok((StatusCode::CREATED, Json(user.into())))
}

pub async fn update_user(
    State(state): State<crate::api::router::AppState>,
    State(db): State<PgPool>,
    axum::extract::Path(id): axum::extract::Path<Uuid>,
    Json(req): Json<UpdateUserRequest>,
) -> Result<Json<UserResponse>, AppError> {
    let user = repository::find_user_by_id(&db, id)
        .await?
        .ok_or_else(|| AppError::not_found("User not found".into()))?;

    let role = match req.role {
        Some(raw) => raw
            .parse::<Role>()
            .map_err(|_| AppError::bad_request("Role must be administrator or demo".into()).with_code("ROLE_INVALID"))?,
        None => user.role.clone(),
    };

    let promoting_demo = user.role == Role::Demo && role == Role::Admin;
    let updated = repository::update_user(&db, id, &role).await?;
    if promoting_demo {
        crate::demo::retire(&state, id).await?;
    }

    Ok(Json(updated.into()))
}

pub async fn delete_user(
    State(db): State<PgPool>,
    State(state): State<crate::api::router::AppState>,
    axum::extract::Path(id): axum::extract::Path<Uuid>,
) -> Result<StatusCode, AppError> {
    crate::demo::retire(&state, id).await?;
    let affected = repository::delete_user(&db, id).await?;

    if affected == 0 {
        return Err(AppError::not_found("User not found".into()));
    }

    Ok(StatusCode::NO_CONTENT)
}

fn generate_tokens(user: &User, config: &Config) -> Result<(String, String), AppError> {
    let now = Utc::now().timestamp() as usize;

    let access_claims = Claims {
        sub: user.id.to_string(),
        exp: now + config.jwt_access_expiry as usize,
        iat: now,
        purpose: TokenPurpose::Access,
    };

    let refresh_claims = Claims {
        sub: user.id.to_string(),
        exp: now + config.jwt_refresh_expiry as usize,
        iat: now,
        purpose: TokenPurpose::Refresh,
    };

    let access_token = encode(
        &Header::default(),
        &access_claims,
        &EncodingKey::from_secret(config.jwt_secret.as_bytes()),
    )
    .db_error("failed to sign access token")?;

    let refresh_token = encode(
        &Header::default(),
        &refresh_claims,
        &EncodingKey::from_secret(config.jwt_secret.as_bytes()),
    )
    .db_error("failed to sign refresh token")?;

    Ok((access_token, refresh_token))
}

fn validate_credentials(username: &str, password: &str) -> Result<(), AppError> {
    if username.trim().is_empty() || username.trim().len() > 255 {
        return Err(AppError::bad_request("Username must contain 1–255 bytes".into()).with_code("USERNAME_INVALID"));
    }
    if !(8..=72).contains(&password.len()) {
        return Err(AppError::bad_request("Password must contain 8–72 bytes".into()).with_code("PASSWORD_INVALID"));
    }
    Ok(())
}

async fn hash_password(password: String) -> Result<String, AppError> {
    tokio::task::spawn_blocking(move || hash(password, DEFAULT_COST))
        .await
        .map_err(|_| AppError::internal("Password hashing task failed".into()))?
        .db_error("failed to hash password")
}
