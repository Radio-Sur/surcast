use sqlx::PgPool;

use surcast_backend::db;

pub async fn setup_db() -> PgPool {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");
    let test_db_url = create_test_db(&database_url).await;
    let pool = db::create_pool(&test_db_url).await;
    db::run_migrations(&pool).await;
    pool
}

fn parse_pg_url(url: &str) -> (String, String, String, u16, String) {
    let rest = url.strip_prefix("postgres://").unwrap_or(url);
    let (userinfo, rest) = rest.split_once('@').unwrap_or(("", rest));
    let (user, pass) = userinfo.split_once(':').unwrap_or((userinfo, ""));
    let (hostport, db) = rest.split_once('/').unwrap_or((rest, ""));
    let (host, port) = hostport.split_once(':').unwrap_or((hostport, "5432"));
    (
        user.to_string(),
        pass.to_string(),
        host.to_string(),
        port.parse().unwrap_or(5432),
        db.to_string(),
    )
}

async fn create_test_db(database_url: &str) -> String {
    let (username, password, host, port, base_db) = parse_pg_url(database_url);

    let admin_url = format!("postgres://{}:{}@{}:{}/postgres", username, password, host, port);
    let admin_pool = db::create_pool(&admin_url).await;

    let test_db_name = format!("{}_test_{}", base_db, uuid::Uuid::new_v4().to_string().replace('-', ""));

    sqlx::query(&format!("DROP DATABASE IF EXISTS \"{}\"", test_db_name))
        .execute(&admin_pool)
        .await
        .ok();

    sqlx::query(&format!("CREATE DATABASE \"{}\"", test_db_name))
        .execute(&admin_pool)
        .await
        .unwrap_or_else(|e| panic!("Failed to create test database '{}': {}", test_db_name, e));

    admin_pool.close().await;

    format!("postgres://{}:{}@{}:{}/{}", username, password, host, port, test_db_name)
}
