use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;

pub async fn create_pool(database_url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(Duration::from_secs(5))
        .acquire_slow_threshold(Duration::from_secs(2))
        .connect(database_url)
        .await
        .expect("Failed to create database pool")
}

pub async fn run_migrations(pool: &PgPool) {
    // Reset migration tracking so modified 001_initial.sql re-applies cleanly.
    // All statements use IF NOT EXISTS / ON CONFLICT DO NOTHING, so re-running is safe.
    sqlx::query("DROP TABLE IF EXISTS _sqlx_migrations CASCADE")
        .execute(pool)
        .await
        .ok();

    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .expect("Failed to run database migrations");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[tokio::test]
    #[ignore]
    async fn test_create_pool_and_run_migrations() {
        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set for this test");
        let pool = create_pool(&database_url).await;
        run_migrations(&pool).await;
        let result: Result<(i64,), _> = sqlx::query_scalar("SELECT COUNT(*) FROM stations").fetch_one(&pool).await;
        assert!(result.is_ok());
    }
}
