//! Shared test infrastructure for the integration tests. The test-database
//! lifecycle itself lives in `surcast_backend::test_db` (visible to both
//! library unit tests and this integration crate); this module re-exports
//! it and keeps the `setup_db()` entry point the test files call.

pub use surcast_backend::test_db::{setup_test_db, TestDb};

/// Creates one isolated, migrated test database and returns its owner.
/// The database is dropped by [`TestDb::cleanup`] — normally via the
/// streamer harness's central teardown; callers without a central runner
/// must call it themselves.
pub async fn setup_db() -> TestDb {
    setup_test_db().await.expect("DATABASE_URL must be set for integration tests")
}
