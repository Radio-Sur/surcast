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
    if req.username.is_empty() || req.password.is_empty() {
        return Err(AppError::BadRequest("Username and password are required".into()));
    }

    let count = repository::count_users(&db).await?;

    if count > 0 {
        return Err(AppError::BadRequest("Setup already completed".into()));
    }

    let password_hash = hash(&req.password, DEFAULT_COST).db_error("failed to hash password")?;

    let user_id = Uuid::new_v4();
    repository::insert_user(&db, user_id, &req.username, &password_hash, &req.username, &Role::Admin).await?;

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
        .ok_or_else(|| AppError::Unauthorized("Invalid username or password".into()))?;

    let valid = verify(&req.password, &user.password_hash).db_error("failed to verify password")?;

    if !valid {
        return Err(AppError::Unauthorized("Invalid username or password".into()));
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
        .ok_or_else(|| AppError::BadRequest("refresh_token required".into()))?;

    let token_data = decode::<Claims>(
        refresh_token,
        &DecodingKey::from_secret(config.jwt_secret.as_bytes()),
        &Validation::default(),
    )
    .map_err(|_| AppError::Unauthorized("Invalid refresh token".into()))?;

    let user_id = Uuid::parse_str(&token_data.claims.sub).map_err(|_| AppError::Unauthorized("Invalid token".into()))?;

    let user = repository::find_user_by_id(&db, user_id)
        .await?
        .ok_or_else(|| AppError::Unauthorized("User not found".into()))?;

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
        .ok_or_else(|| AppError::NotFound("User not found".into()))?;

    Ok(Json(user.into()))
}

pub async fn list_users(State(db): State<PgPool>) -> Result<Json<Vec<UserResponse>>, AppError> {
    let users = repository::find_all_users(&db).await?;

    Ok(Json(users.into_iter().map(|u| u.into()).collect()))
}

pub async fn update_user(
    State(db): State<PgPool>,
    axum::extract::Path(id): axum::extract::Path<Uuid>,
    Json(req): Json<UpdateUserRequest>,
) -> Result<Json<UserResponse>, AppError> {
    let user = repository::find_user_by_id(&db, id)
        .await?
        .ok_or_else(|| AppError::NotFound("User not found".into()))?;

    let name = req.name.unwrap_or(user.name);
    let role = req.role.unwrap_or(user.role);

    let updated = repository::update_user(&db, id, &name, &role).await?;

    Ok(Json(updated.into()))
}

pub async fn delete_user(State(db): State<PgPool>, axum::extract::Path(id): axum::extract::Path<Uuid>) -> Result<StatusCode, AppError> {
    let affected = repository::delete_user(&db, id).await?;

    if affected == 0 {
        return Err(AppError::NotFound("User not found".into()));
    }

    Ok(StatusCode::NO_CONTENT)
}

fn generate_tokens(user: &User, config: &Config) -> Result<(String, String), AppError> {
    let now = Utc::now().timestamp() as usize;

    let access_claims = Claims {
        sub: user.id.to_string(),
        exp: now + config.jwt_access_expiry as usize,
        iat: now,
    };

    let refresh_claims = Claims {
        sub: user.id.to_string(),
        exp: now + config.jwt_refresh_expiry as usize,
        iat: now,
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
