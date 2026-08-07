use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::errors::{AppError, DbResult};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum IcecastMode {
    Managed,
    External,
}

#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct IcecastSettings {
    pub id: Uuid,
    pub enabled: bool,
    pub mode: IcecastMode,
    pub port: i32,
    pub source_password: String,
    pub admin_user: String,
    pub admin_password: String,
    pub external_url: Option<String>,
    pub external_source_pw: Option<String>,
    pub external_admin_pw: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Default, Deserialize)]
pub struct IcecastSettingsUpdate {
    pub enabled: Option<bool>,
    pub mode: Option<IcecastMode>,
    pub port: Option<i32>,
    pub source_password: Option<String>,
    pub admin_user: Option<String>,
    pub admin_password: Option<String>,
    pub external_url: Option<String>,
    pub external_source_pw: Option<String>,
    pub external_admin_pw: Option<String>,
}

pub async fn get_settings(pool: &PgPool) -> Result<IcecastSettings, AppError> {
    sqlx::query_as::<_, IcecastSettings>("SELECT * FROM icecast_settings ORDER BY id LIMIT 1")
        .fetch_one(pool)
        .await
        .db_error("failed to read icecast settings")
}

pub async fn get_connection_config(pool: &PgPool) -> Result<(String, String), AppError> {
    let settings = get_settings(pool).await?;
    if settings.mode == IcecastMode::External {
        let url = settings.external_url.clone().unwrap_or_default();
        let pw = settings.external_source_pw.clone().unwrap_or(settings.source_password);
        Ok((url, pw))
    } else {
        let addr = format!("127.0.0.1:{}", settings.port);
        Ok((addr, settings.source_password.clone()))
    }
}

pub async fn update_settings(pool: &PgPool, update: &IcecastSettingsUpdate) -> Result<IcecastSettings, AppError> {
    let current = get_settings(pool).await?;

    let enabled = update.enabled.unwrap_or(current.enabled);
    let mode = update.mode.clone().unwrap_or(current.mode);
    let port = update.port.unwrap_or(current.port);
    let source_password = update.source_password.clone().unwrap_or(current.source_password);
    let admin_user = update.admin_user.clone().unwrap_or(current.admin_user);
    let admin_password = update.admin_password.clone().unwrap_or(current.admin_password);
    let external_url = update.external_url.clone().or(current.external_url);
    let external_source_pw = update.external_source_pw.clone().or(current.external_source_pw);
    let external_admin_pw = update.external_admin_pw.clone().or(current.external_admin_pw);

    sqlx::query(
        r#"UPDATE icecast_settings SET
           enabled = $1, mode = $2, port = $3, source_password = $4,
           admin_user = $5, admin_password = $6,
           external_url = $7, external_source_pw = $8, external_admin_pw = $9,
           updated_at = NOW()
           WHERE id = '00000000-0000-0000-0000-000000000001'"#,
    )
    .bind(enabled)
    .bind(&mode)
    .bind(port)
    .bind(&source_password)
    .bind(&admin_user)
    .bind(&admin_password)
    .bind(&external_url)
    .bind(&external_source_pw)
    .bind(&external_admin_pw)
    .execute(pool)
    .await
    .db_error("failed to update icecast settings")?;

    get_settings(pool).await
}
