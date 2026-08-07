use axum::extract::State;
use axum::http::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use axum::Json;
use jsonwebtoken::{decode, DecodingKey, Validation};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

#[derive(Debug)]
pub struct TokenVerificationFailed;

/// Verify a JWT token string and return the AuthUser. Does NOT support API keys.
pub fn verify_token(token: &str, jwt_secret: &str) -> Result<AuthUser, TokenVerificationFailed> {
    let token_data = decode::<Claims>(token, &DecodingKey::from_secret(jwt_secret.as_bytes()), &Validation::default())
        .map_err(|_| TokenVerificationFailed)?;

    let user_id = Uuid::parse_str(&token_data.claims.sub).map_err(|_| TokenVerificationFailed)?;
    Ok(AuthUser {
        id: user_id,
        role: Role::Viewer,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};

    fn make_token(sub: &str, secret: &str, exp: usize) -> String {
        let claims = Claims {
            sub: sub.to_string(),
            exp,
            iat: 1_000_000_000,
        };
        encode(&Header::default(), &claims, &EncodingKey::from_secret(secret.as_bytes())).unwrap()
    }

    #[test]
    fn test_verify_token_valid() {
        let sub = "550e8400-e29b-41d4-a716-446655440000";
        let secret = "mysecret";
        let token = make_token(sub, secret, 9_999_999_999);
        let result = verify_token(&token, secret);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().id.to_string(), sub);
    }

    #[test]
    fn test_verify_token_expired() {
        let secret = "mysecret";
        let token = make_token("550e8400-e29b-41d4-a716-446655440000", secret, 1_000_000_000);
        let result = verify_token(&token, secret);
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_token_wrong_secret() {
        let secret = "mysecret";
        let token = make_token("550e8400-e29b-41d4-a716-446655440000", secret, 9_999_999_999);
        let result = verify_token(&token, "wrongsecret");
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_token_malformed() {
        let result = verify_token("not.a.token", "mysecret");
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_token_no_sub() {
        let secret = "mysecret";
        let token = make_token("not-a-uuid", secret, 9_999_999_999);
        let result = verify_token(&token, secret);
        assert!(result.is_err());
    }
}

use crate::api::AppState;
use crate::auth::models::Claims;
use crate::auth::models::Role;

#[derive(Debug, Clone)]
pub struct AuthUser {
    pub id: Uuid,
    pub role: Role,
}

pub async fn auth_middleware(State(state): State<AppState>, mut req: Request<axum::body::Body>, next: Next) -> Result<Response, Response> {
    let auth_header = req
        .headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| (StatusCode::UNAUTHORIZED, Json(json!({ "error": "Missing Authorization header" }))).into_response())?;

    let token = auth_header.strip_prefix("Bearer ").ok_or_else(|| {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "Invalid Authorization header format" })),
        )
            .into_response()
    })?;

    if token.starts_with("sur_") {
        let hash = hex::encode(Sha256::digest(token.as_bytes()));

        let key_row = sqlx::query_as::<_, (Uuid, Uuid, Role)>(
            "SELECT ak.id, u.id, u.role FROM api_keys ak JOIN users u ON u.id = ak.user_id WHERE ak.key_hash = $1 AND ak.is_active = true AND (ak.expires_at IS NULL OR ak.expires_at > NOW())",
        )
        .bind(&hash)
        .fetch_optional(&state.db)
        .await
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Database error" })),
            )
                .into_response()
        })?
        .ok_or_else(|| {
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Invalid or inactive API key" })),
            )
                .into_response()
        })?;

        let (_key_id, user_id, role) = key_row;

        if let Err(e) = sqlx::query("UPDATE api_keys SET last_used_at = NOW() WHERE key_hash = $1")
            .bind(&hash)
            .execute(&state.db)
            .await
        {
            tracing::warn!("Failed to update API key last_used_at: {e}");
        }

        let auth_user = AuthUser { id: user_id, role };
        req.extensions_mut().insert(auth_user);
        Ok(next.run(req).await)
    } else {
        let token_data = decode::<Claims>(
            token,
            &DecodingKey::from_secret(state.config.jwt_secret.as_bytes()),
            &Validation::default(),
        )
        .map_err(|_| (StatusCode::UNAUTHORIZED, Json(json!({ "error": "Invalid or expired token" }))).into_response())?;

        let user_id = Uuid::parse_str(&token_data.claims.sub)
            .map_err(|_| (StatusCode::UNAUTHORIZED, Json(json!({ "error": "Invalid token payload" }))).into_response())?;

        let user = sqlx::query_as::<_, (Uuid, Role)>("SELECT id, role FROM users WHERE id = $1")
            .bind(user_id)
            .fetch_optional(&state.db)
            .await
            .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": "Database error" }))).into_response())?
            .ok_or_else(|| (StatusCode::UNAUTHORIZED, Json(json!({ "error": "User not found" }))).into_response())?;

        let (id, role) = user;

        let auth_user = AuthUser { id, role };
        req.extensions_mut().insert(auth_user);
        Ok(next.run(req).await)
    }
}

pub async fn require_admin(Extension(user): Extension<AuthUser>, req: Request<axum::body::Body>, next: Next) -> Result<Response, Response> {
    if user.role != Role::Admin {
        return Err((StatusCode::FORBIDDEN, Json(json!({ "error": "Admin access required" }))).into_response());
    }
    Ok(next.run(req).await)
}
