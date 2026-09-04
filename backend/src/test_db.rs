//! Shared test-database lifecycle for the whole backend test suite.
//!
//! Every temporary database is created in a strictly validated, fixed-length
//! format and owned by a [`TestDb`] handle, so normal test completion drops
//! it immediately (the streamer harness drops it in its central teardown;
//! custom unit tests go through [`run_with_test_db`], which drops it on
//! success AND on scenario panic). A SIGKILL or an aborted `cargo test` can
//! still orphan a database; the stale sweep ([`sweep_stale_test_dbs`])
//! recognizes orphans by name and age and removes them on the next
//! test-database setup — never touching databases that are not ours, never
//! dropping a database a parallel test process may still own.
//!
//! Naming (see [`is_legacy_test_db_name`] / [`parse_new_test_db_name`]):
//! - legacy (created by the old `tests/common` helper, no longer produced):
//!   `<base>_test_<32 hex chars>` — a migration orphan, always a candidate;
//! - current: `scdb2_<16-hex base fingerprint>_<10-digit unix seconds>_<8-hex pid>_<16-hex random>`
//!   — every field is fixed-width, so the name length NEVER depends on the
//!   base database name. The maximum length is 59 ASCII bytes, well below
//!   PostgreSQL's `max_identifier_length` (63 bytes), even for a 200-char
//!   base, a 10-digit timestamp and a full-width PID. The base fingerprint
//!   (first 8 bytes of SHA-256 of the base name) is stable across processes
//!   and runs, so the sweep recognizes its own databases after a restart;
//!   the random suffix (64 bits) is the uniqueness source; the timestamp
//!   encodes the age, so a fresh database of a parallel process is never a
//!   candidate; the PID is diagnostics only.
//!
//! `#[sqlx::test]`-managed databases (`_sqlx_test_*`) belong to SQLx's own
//! lifecycle: a successful test drops its database, a failed/panicking test
//! keeps it for debugging and the next run of the same test binary cleans it
//! up. The custom sweep NEVER matches those names — two ownership systems,
//! deliberately separate.

use std::ops::Deref;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::FutureExt;
use sha2::Digest;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;

use crate::db;

/// Databases younger than this are never swept automatically. A parallel
/// `cargo test` process may legitimately own a database that currently has
/// no open connection (e.g. between scenario steps), so age is the only
/// reliable liveness signal; the full test suite runs in well under an
/// hour, so one day is a comfortable safety margin that still keeps daily
/// runs free of yesterday's orphans.
const STALE_AGE: Duration = Duration::from_secs(24 * 60 * 60);

/// Separator between the base database name and the test suffix in the
/// legacy format (created by the old `tests/common` helper).
const TEST_SEPARATOR: &str = "_test_";

/// Prefix of the current fixed-length format (Surcast test DataBase v2).
const NEW_PREFIX: &str = "scdb2_";

/// Prefix for the template database used to speed up per-test creation.

// Fixed field widths of the current format. The total budget is
// `NEW_PREFIX (6) + fingerprint (16) + 1 + timestamp (10) + 1 + pid (8) + 1
// + random (16) = 59` ASCII bytes, independent of the base database name.
const NEW_FINGERPRINT_LEN: usize = 16;
const NEW_TIMESTAMP_LEN: usize = 10;
const NEW_PID_LEN: usize = 8;
const NEW_RANDOM_LEN: usize = 16;

/// Maximum length of a current-format name (ASCII bytes). PostgreSQL
/// truncates identifiers longer than `max_identifier_length` (63 bytes),
/// which would break the parser and could silently drop the random suffix;
/// the budget is a hard cap asserted by tests.
const NEW_NAME_MAX_LEN: usize = NEW_PREFIX.len() + NEW_FINGERPRINT_LEN + 1 + NEW_TIMESTAMP_LEN + 1 + NEW_PID_LEN + 1 + NEW_RANDOM_LEN;

/// The parsed shape of a current-format test database name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTestDbName {
    /// Unix seconds encoded in the name (creation time of the database).
    pub timestamp: u64,
    /// PID of the process that created it (diagnostics only).
    pub pid: u32,
}

/// Legacy format: `<base>_test_` followed by exactly 32 hex characters and
/// nothing else. The old harness never dropped these, so they are migration
/// orphans; this version never creates them. Truncated legacy names (base
/// long enough that PostgreSQL cut the suffix) are NOT guessed at — safety
/// beats heuristic cleanup.
pub fn is_legacy_test_db_name(candidate: &str, base: &str) -> bool {
    match candidate.strip_prefix(&format!("{base}{TEST_SEPARATOR}")) {
        Some(rest) => is_hex32(rest),
        None => false,
    }
}

/// Current format: `scdb2_<16-hex base fingerprint>_<10-digit unix
/// seconds>_<8-hex pid>_<16-hex random>`. Returns the embedded
/// timestamp/pid, or `None` when the name is not a valid current-format
/// test database for this base. The fingerprint binds the name to exactly
/// one base database, so the sweep can never match another base's (or a
/// manual) database.
pub fn parse_new_test_db_name(candidate: &str, base: &str) -> Option<NewTestDbName> {
    // Anything longer than the fixed budget cannot be a current-format name
    // (PostgreSQL would have truncated it at 63 bytes); rejecting it up
    // front also keeps the parser aligned with the generator's length cap.
    if candidate.len() > NEW_NAME_MAX_LEN {
        return None;
    }
    let rest = candidate.strip_prefix(NEW_PREFIX)?;
    let (fingerprint, rest) = rest.split_once('_')?;
    if fingerprint.len() != NEW_FINGERPRINT_LEN || u64::from_str_radix(fingerprint, 16).ok()? != base_fingerprint(base) {
        return None;
    }
    let (timestamp, rest) = rest.split_once('_')?;
    if timestamp.len() != NEW_TIMESTAMP_LEN || !is_digits(timestamp) {
        return None;
    }
    let (pid, random) = rest.split_once('_')?;
    if pid.len() != NEW_PID_LEN || !pid.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    if random.len() != NEW_RANDOM_LEN || !random.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    Some(NewTestDbName {
        timestamp: timestamp.parse().ok()?,
        pid: u32::from_str_radix(pid, 16).ok()?,
    })
}

/// Any name this harness recognizes as a test database of `base` (legacy or
/// current format).
pub fn is_test_db_name(candidate: &str, base: &str) -> bool {
    is_legacy_test_db_name(candidate, base) || parse_new_test_db_name(candidate, base).is_some()
}

fn is_hex32(value: &str) -> bool {
    value.len() == 32 && value.chars().all(|c| c.is_ascii_hexdigit())
}

fn is_digits(value: &str) -> bool {
    !value.is_empty() && value.chars().all(|c| c.is_ascii_digit())
}

/// Stable fingerprint of the base database name: the first 8 bytes of
/// SHA-256 (16 hex chars). SHA-256 is a fixed public standard and already a
/// dependency, so the value is identical across processes, runs and
/// compilers — unlike `DefaultHasher`, whose output is not a naming
/// contract.
fn base_fingerprint(base: &str) -> u64 {
    let digest = sha2::Sha256::digest(base.as_bytes());
    u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 output is 32 bytes"))
}

/// Deterministic current-format name from explicit parts. The clock, the
/// process id and the randomness live in [`new_test_db_name`]; this split
/// lets tests control the timestamp (stale-age regression) and push extreme
/// inputs (long base, `u32::MAX` pid) without building an ID framework.
fn new_test_db_name_with(base: &str, timestamp: u64, pid: u32, random: &str) -> String {
    format!("{NEW_PREFIX}{:016x}_{timestamp}_{pid:08x}_{random}", base_fingerprint(base))
}

/// A unique fresh-format name for `base`. The fixed-width fields keep the
/// total at [`NEW_NAME_MAX_LEN`] bytes no matter how long the base name is;
/// the random 16-hex suffix (64 bits) is the uniqueness source, the PID is
/// diagnostics only.
fn new_test_db_name(base: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs();
    let pid = std::process::id();
    let random: String = uuid::Uuid::new_v4().simple().to_string()[..NEW_RANDOM_LEN].to_owned();
    new_test_db_name_with(base, timestamp, pid, &random)
}

/// PostgreSQL quoted-identifier quoting: wraps the name in `"` and doubles
/// embedded `"` (SQL standard). Used for every interpolated database name
/// (CREATE, cleanup DROP, sweep DROP, regression DROP) — Rust's `Debug`
/// formatting is NOT a SQL quoting contract, and legacy candidates derive
/// from the base name, which may legally contain quote characters.
fn quote_pg_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Drops a test database from the maintenance connection. With `terminate`,
/// straggler backends are killed first (needed when a target pool was
/// opened and must be forced out: [`TestDb::cleanup`], partial-setup
/// rollback, regression cleanup). The sweep passes `false` — it already
/// verified no active backend exists and the plain DROP is the source of
/// truth. The DROP error is returned, never swallowed.
async fn drop_test_database(admin_pool: &PgPool, db_name: &str, terminate: bool) -> Result<(), sqlx::Error> {
    if terminate {
        let _ = sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()")
            .bind(db_name)
            .execute(admin_pool)
            .await;
    }
    sqlx::query(&format!("DROP DATABASE IF EXISTS {}", quote_pg_identifier(db_name)))
        .execute(admin_pool)
        .await
        .map(|_| ())
}

/// Owns one isolated, migrated test database. The target pool is closed and
/// the database dropped from the maintenance database (`postgres`), never
/// from the target itself.
pub struct TestDb {
    pub pool: PgPool,
    admin_pool: PgPool,
    pub db_name: String,
}

impl TestDb {
    /// Closes the target pool first, terminates any straggler backends that
    /// outlive the pool close (a runtime queue load can leave an idle
    /// connection whose backend outlives its pool handle), then drops the
    /// database. The admin pool is closed even when the DROP fails, and the
    /// DROP error is returned, not swallowed.
    pub async fn cleanup(self) -> Result<(), String> {
        self.pool.close().await;
        let drop_result = drop_test_database(&self.admin_pool, &self.db_name, true).await;
        self.admin_pool.close().await;
        drop_result.map_err(|error| format!("failed to drop test database '{}': {error}", self.db_name))
    }
}

/// Test databases are usable as plain `PgPool`s everywhere a pool is
/// expected (`&TestDb` coerces to `&PgPool`).
impl Deref for TestDb {
    type Target = PgPool;

    fn deref(&self) -> &PgPool {
        &self.pool
    }
}

fn options_from_env() -> Option<PgConnectOptions> {
    let database_url = std::env::var("DATABASE_URL").ok()?;
    Some(PgConnectOptions::from_str(&database_url).unwrap_or_else(|error| panic!("invalid DATABASE_URL: {error}")))
}

/// Parsed `DATABASE_URL` connection options (the real `PgConnectOptions`,
/// like the provisioner uses). Exposed for test-side diagnostics (e.g.
/// checking that a database no longer exists).
pub fn connection_options() -> Option<PgConnectOptions> {
    options_from_env()
}

/// Failure of the post-CREATE initialization that [`create_and_init`] can
/// observe; the migration path panics instead and is handled by the unwind
/// boundary.
enum SetupError {
    Connect(sqlx::Error),
}

/// Default initialization of a freshly created test database: connect the
/// target pool (max 5 connections) and run the migrations. Migration
/// failures panic (see [`db::run_migrations`]); the unwind boundary in
/// [`create_and_init`] rolls the database back.
async fn default_init(options: &PgConnectOptions, db_name: &str) -> Result<PgPool, SetupError> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect_with(options.clone().database(db_name))
        .await
        .map_err(SetupError::Connect)?;
    // If DB was created from template it already has tables - skip migrations
    let has_tables: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='stations')")
            .fetch_one(&pool)
            .await
            .unwrap_or(false);
    if !has_tables {
        db::run_migrations(&pool).await;
    }
    Ok(pool)
}

/// Creates the database, runs `init` and returns the owned [`TestDb`].
/// From the successful `CREATE DATABASE` on, EVERY failure path rolls the
/// freshly created database back before panicking — a fresh name carries a
/// recent timestamp, so the stale sweep would otherwise leave it alone for
/// [`STALE_AGE`], recreating the original leak. `init` is a parameter so
/// the partial-setup regression can inject a controlled failure; production
/// uses [`default_init`].
async fn create_and_init(
    options: &PgConnectOptions,
    db_name: &str,
    init: impl AsyncFnOnce(&PgConnectOptions, &str) -> Result<PgPool, SetupError>,
) -> TestDb {
    let admin_pool = PgPoolOptions::new()
        .connect_with(options.clone().database("postgres"))
        .await
        .unwrap_or_else(|error| panic!("failed to connect for test database setup: {error}"));
    let create_sql = format!("CREATE DATABASE {}", quote_pg_identifier(db_name));
    if let Err(error) = sqlx::query(&create_sql).execute(&admin_pool).await {
        admin_pool.close().await;
        panic!("failed to create test database '{db_name}': {error}");
    }

    let init = std::panic::AssertUnwindSafe(init(options, db_name)).catch_unwind().await;
    match init {
        Ok(Ok(pool)) => TestDb {
            pool,
            admin_pool,
            db_name: db_name.to_owned(),
        },
        Ok(Err(SetupError::Connect(error))) => {
            let drop_result = drop_test_database(&admin_pool, db_name, true).await;
            admin_pool.close().await;
            match drop_result {
                Ok(()) => panic!("failed to connect to test database '{db_name}': {error}"),
                Err(drop_error) => panic!(
                    "failed to connect to test database '{db_name}': {error}; additionally failed to roll back the freshly created database: {drop_error}"
                ),
            }
        }
        Err(panic) => {
            let drop_result = drop_test_database(&admin_pool, db_name, true).await;
            admin_pool.close().await;
            if let Err(drop_error) = drop_result {
                eprintln!("[test-db] setup rollback after panic failed for '{db_name}': {drop_error}");
            }
            std::panic::resume_unwind(panic);
        }
    }
}

/// Creates one isolated, migrated test database and returns its owner.
/// Returns `None` only when `DATABASE_URL` is absent (tests then skip); any
/// configured connection, setup or migration failure is a test failure,
/// never a silent skip. Runs the stale sweep once per process before the
/// first database is created.
pub async fn setup_test_db() -> Option<TestDb> {
    sweep_once().await;
    let options = options_from_env()?;
    let base = options.get_database().map(str::to_owned).unwrap_or_else(|| "postgres".to_owned());
    let db_name = new_test_db_name(&base);
    Some(create_and_init(&options, &db_name, async |options, db_name| default_init(options, db_name).await).await)
}

/// Runs `scenario` against a freshly created, migrated test database and
/// guarantees the database is dropped afterwards:
/// - scenario panic → the database is dropped FIRST, then the original
///   panic is resumed (a cleanup failure is printed, never merged into the
///   original panic);
/// - clean scenario → a cleanup failure panics with the cleanup error;
/// - `DATABASE_URL` absent → the scenario is skipped.
///
/// The database handle stays in the runner's frame, so a panic inside the
/// scenario can never skip the cleanup.
pub async fn run_with_test_db(scenario: impl AsyncFnOnce(&TestDb) -> ()) {
    let Some(db) = setup_test_db().await else { return };
    let outcome = std::panic::AssertUnwindSafe(async {
        scenario(&db).await;
    })
    .catch_unwind()
    .await;
    let cleanup = db.cleanup().await;
    match outcome {
        Ok(()) => cleanup.unwrap_or_else(|error| panic!("test database cleanup failed: {error}")),
        Err(panic) => {
            if let Err(error) = cleanup {
                eprintln!("[test-db] cleanup after scenario panic failed: {error}");
            }
            std::panic::resume_unwind(panic);
        }
    }
}

/// Outcome of one stale sweep, for the diagnostics log and the report.
#[derive(Debug, Default)]
pub struct SweepReport {
    /// Validated stale candidates found (legacy + aged current format).
    pub candidates_found: usize,
    pub candidates_size_bytes: i64,
    pub deleted: usize,
    pub deleted_size_bytes: i64,
    /// Skipped because a live backend was connected.
    pub skipped_active: usize,
    /// Skipped because the DROP itself failed.
    pub skipped_errors: usize,
}

/// Lists every database, strictly validates the names, and drops the stale
/// ones (legacy orphans always; current-format ones older than
/// [`STALE_AGE`]). Active databases are never dropped and `FORCE` is never
/// used; a database that refuses a plain DROP is skipped and logged. Safe
/// to run while other test processes are alive: a fresh parallel database
/// carries a recent timestamp and is never a candidate. `_sqlx_test_*`
/// databases are NOT matched — they belong to SQLx's own lifecycle.
pub async fn sweep_stale_test_dbs() -> Option<SweepReport> {
    let options = options_from_env()?;
    let base = options.get_database().map(str::to_owned).unwrap_or_else(|| "postgres".to_owned());
    let admin_pool = PgPoolOptions::new()
        .connect_with(options.database("postgres"))
        .await
        .unwrap_or_else(|error| panic!("failed to connect for stale sweep: {error}"));
    let report = sweep_with_pool(&admin_pool, &base).await;
    admin_pool.close().await;
    Some(report)
}

async fn sweep_with_pool(admin_pool: &PgPool, base: &str) -> SweepReport {
    let mut report = SweepReport::default();
    let names: Vec<String> = sqlx::query_scalar("SELECT datname FROM pg_database")
        .fetch_all(admin_pool)
        .await
        .unwrap_or_else(|error| panic!("failed to list databases for stale sweep: {error}"));

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs();

    for name in names {
        let stale = if is_legacy_test_db_name(&name, base) {
            // No longer produced by this harness: migration orphans.
            true
        } else if let Some(parsed) = parse_new_test_db_name(&name, base) {
            now.saturating_sub(parsed.timestamp) >= STALE_AGE.as_secs()
        } else {
            continue;
        };
        if !stale {
            continue;
        }

        let size: i64 = sqlx::query_scalar("SELECT pg_database_size($1)")
            .bind(&name)
            .fetch_one(admin_pool)
            .await
            .unwrap_or(0);
        report.candidates_found += 1;
        report.candidates_size_bytes += size;

        let active: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid())")
                .bind(&name)
                .fetch_one(admin_pool)
                .await
                .unwrap_or(true);
        if active {
            eprintln!("[test-db] sweep: skipping active database {name}");
            report.skipped_active += 1;
            continue;
        }

        match sqlx::query(&format!("DROP DATABASE IF EXISTS {}", quote_pg_identifier(&name)))
            .execute(admin_pool)
            .await
        {
            Ok(_) => {
                report.deleted += 1;
                report.deleted_size_bytes += size;
            }
            Err(error) => {
                eprintln!("[test-db] sweep: drop failed for {name}: {error}");
                report.skipped_errors += 1;
            }
        }
    }
    report
}

static SWEEP_DONE: std::sync::Once = std::sync::Once::new();
static SWEEP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Runs the stale sweep exactly once per process, before the first test
/// database is created. Concurrent `setup_test_db` calls (tests run in
/// parallel threads) serialize on [`SWEEP_LOCK`].
async fn sweep_once() {
    if SWEEP_DONE.is_completed() {
        return;
    }
    let _guard = SWEEP_LOCK.lock().await;
    if SWEEP_DONE.is_completed() {
        return;
    }
    if let Some(report) = sweep_stale_test_dbs().await {
        eprintln!(
            "[test-db] stale sweep: {} candidates / {} bytes; deleted {} / {} bytes; {} active, {} errors",
            report.candidates_found,
            report.candidates_size_bytes,
            report.deleted,
            report.deleted_size_bytes,
            report.skipped_active,
            report.skipped_errors,
        );
    }
    SWEEP_DONE.call_once(|| {});
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    const BASE: &str = "surcast";
    static HEX32: &str = "0123456789abcdef0123456789abcdef";
    static HEX16: &str = "0123456789abcdef";

    fn hex32_suffix(suffix: &str) -> String {
        format!("{BASE}{TEST_SEPARATOR}{suffix}")
    }

    fn new_name(timestamp: u64, pid: u32, random: &str) -> String {
        new_test_db_name_with(BASE, timestamp, pid, random)
    }

    #[test]
    fn legacy_name_validation_accepts_exact_32_hex_only() {
        assert!(is_legacy_test_db_name(&hex32_suffix(HEX32), BASE));
        // Not our base.
        assert!(!is_legacy_test_db_name(&format!("otherbase{TEST_SEPARATOR}{HEX32}"), BASE));
        // No suffix, manual names, plain base names.
        assert!(!is_legacy_test_db_name(&hex32_suffix(""), BASE));
        assert!(!is_legacy_test_db_name(&hex32_suffix("manual"), BASE));
        assert!(!is_legacy_test_db_name(BASE, BASE));
        // Wrong hex length or content.
        assert!(!is_legacy_test_db_name(&hex32_suffix(&HEX32[..31]), BASE));
        assert!(!is_legacy_test_db_name(&hex32_suffix(&format!("{HEX32}0")), BASE));
        assert!(!is_legacy_test_db_name(&hex32_suffix("zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"), BASE));
        // System databases and the current format.
        for protected in ["postgres", "template0", "template1"] {
            assert!(!is_legacy_test_db_name(protected, BASE));
        }
        assert!(!is_legacy_test_db_name(&new_name(1_234_567_890, 0x1234_5678, HEX16), BASE));
    }

    #[test]
    fn new_name_validation_accepts_structured_current_format_only() {
        let valid = new_name(1_234_567_890, 0x1234_5678, HEX16);
        let parsed = parse_new_test_db_name(&valid, BASE).expect("valid current-format name must parse");
        assert_eq!(parsed.timestamp, 1_234_567_890);
        assert_eq!(parsed.pid, 0x1234_5678);

        // Timestamp must be exactly 10 digits.
        assert!(parse_new_test_db_name(&new_name(123_456_789, 0x1234_5678, HEX16), BASE).is_none());
        assert!(parse_new_test_db_name(&new_name(12_345_678_901, 0x1234_5678, HEX16), BASE).is_none());
        // PID must be exactly 8 hex chars — the generator zero-pads any
        // u32 (`{:08x}`), so a 9-char pid is impossible via the generator
        // and is constructed raw.
        let long_pid = format!("{NEW_PREFIX}{:016x}_{}_123456789_{HEX16}", base_fingerprint(BASE), 1_234_567_890);
        assert!(parse_new_test_db_name(&long_pid, BASE).is_none());
        // Random suffix must be exactly 16 hex chars with nothing after it.
        assert!(parse_new_test_db_name(&new_name(1_234_567_890, 0x1234_5678, &HEX16[..15]), BASE).is_none());
        assert!(parse_new_test_db_name(&new_name(1_234_567_890, 0x1234_5678, &format!("{HEX16}0")), BASE).is_none());
        assert!(parse_new_test_db_name(&format!("{valid}extra"), BASE).is_none());
        // Wrong fingerprint (different base) never parses.
        assert!(parse_new_test_db_name(&valid, "otherbase").is_none());
        assert!(parse_new_test_db_name(&valid, "surcasx").is_none());
        // Legacy names never parse as current format and vice versa.
        assert!(parse_new_test_db_name(&hex32_suffix(HEX32), BASE).is_none());
        assert!(!is_legacy_test_db_name(&valid, BASE));
    }

    #[test]
    fn fresh_names_are_recognized_and_self_describing() {
        let name = new_test_db_name(BASE);
        let parsed = parse_new_test_db_name(&name, BASE).expect("generated name must parse");
        assert_eq!(parsed.pid, std::process::id());
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_secs();
        assert!(
            now.saturating_sub(parsed.timestamp) < 60,
            "fresh name must carry a recent timestamp"
        );
        assert!(is_test_db_name(&name, BASE));
        assert_eq!(name.len(), NEW_NAME_MAX_LEN, "generated names must use the fixed-length budget");
    }

    /// `[test-db]` databases are only swept when their embedded age exceeds
    /// the stale window — a fresh parallel process's database is never a
    /// candidate.
    #[test]
    fn stale_age_threshold_uses_embedded_timestamp() {
        let fresh_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_secs();
        let old_ts = fresh_ts - STALE_AGE.as_secs() - 60;
        let fresh = parse_new_test_db_name(&new_name(fresh_ts, 1, HEX16), BASE).unwrap();
        assert_eq!(fresh_ts.saturating_sub(fresh.timestamp), 0);
        let old = parse_new_test_db_name(&new_name(old_ts, 1, HEX16), BASE).unwrap();
        assert!(fresh_ts.saturating_sub(old.timestamp) >= STALE_AGE.as_secs());
    }

    /// The current-format generator never collides within one process
    /// (random 16-hex suffix, 64 bits).
    #[test]
    fn generated_names_are_unique() {
        let mut seen = HashSet::new();
        for _ in 0..200 {
            assert!(seen.insert(new_test_db_name(BASE)));
        }
    }

    /// The whole point of the fixed-length redesign: a 200-char base, a
    /// 10-digit timestamp and a full-width PID still produce a name below
    /// PostgreSQL's 63-byte identifier limit — and it parses back with the
    /// correct fingerprint, so the sweep recognizes it after a restart.
    #[test]
    fn long_base_and_max_pid_names_stay_within_postgres_identifier_limit() {
        let long_base = "a".repeat(200);
        let name = new_test_db_name_with(&long_base, 9_999_999_999, u32::MAX, HEX16);
        assert_eq!(name.len(), NEW_NAME_MAX_LEN, "length must be fixed, not base-dependent");
        assert!(name.len() < 63, "must stay below max_identifier_length");
        let parsed = parse_new_test_db_name(&name, &long_base).expect("long-base name must parse back");
        assert_eq!(parsed.timestamp, 9_999_999_999);
        assert_eq!(parsed.pid, u32::MAX);
        // The fingerprint identifies THIS base and no other.
        assert!(parse_new_test_db_name(&name, "otherbase").is_none());
        // Length is independent of the base length (empty base included).
        assert_eq!(new_test_db_name_with("", 9_999_999_999, u32::MAX, HEX16).len(), NEW_NAME_MAX_LEN);
    }

    #[test]
    fn base_fingerprint_is_stable_and_distinguishes_bases() {
        assert_eq!(base_fingerprint(BASE), base_fingerprint(BASE));
        assert_ne!(base_fingerprint(BASE), base_fingerprint("surcasx"));
    }

    #[test]
    fn quote_pg_identifier_escapes_double_quotes() {
        assert_eq!(quote_pg_identifier("simple"), "\"simple\"");
        assert_eq!(quote_pg_identifier("a\"b"), "\"a\"\"b\"");
        assert_eq!(quote_pg_identifier(""), "\"\"");
    }

    async fn admin_pool_from_env(options: &PgConnectOptions) -> PgPool {
        PgPoolOptions::new()
            .connect_with(options.clone().database("postgres"))
            .await
            .expect("admin connection must work")
    }

    // ---- Partial-setup rollback regression -------------------------------

    /// Regression for the pre-fix leak: from the successful CREATE on, no
    /// failure path may leave the fresh database behind. An injected
    /// target-connect failure must panic with the ORIGINAL connect error
    /// AND roll the database back immediately.
    #[tokio::test]
    async fn partial_init_failure_rolls_back_the_fresh_database() {
        let Some(options) = options_from_env() else { return };
        let base = options.get_database().map(str::to_owned).unwrap_or_else(|| "postgres".to_owned());
        let db_name = new_test_db_name(&base);

        let outcome = std::panic::AssertUnwindSafe(create_and_init(
            &options,
            &db_name,
            async |_options, _db_name| -> Result<PgPool, SetupError> {
                Err(SetupError::Connect(sqlx::Error::Protocol("injected connect failure".into())))
            },
        ))
        .catch_unwind()
        .await;

        let admin_pool = admin_pool_from_env(&options).await;
        let exists = database_exists(&admin_pool, &db_name).await;
        let cleanup = drop_test_database(&admin_pool, &db_name, true).await;
        admin_pool.close().await;
        cleanup.expect("regression cleanup");

        let panic = outcome.err().expect("the injected init failure must panic");
        let message = panic.downcast_ref::<String>().expect("panic payload must be a String");
        assert!(
            message.contains("injected connect failure"),
            "the original connect error must be reported: {message}"
        );
        assert!(!exists, "the partially initialized database must be rolled back");
    }

    /// The migration/init path can PANIC between CREATE and the returned
    /// owner; the unwind boundary must roll the database back and resume
    /// the ORIGINAL panic payload.
    #[tokio::test]
    async fn partial_init_panic_rolls_back_the_fresh_database() {
        let Some(options) = options_from_env() else { return };
        let base = options.get_database().map(str::to_owned).unwrap_or_else(|| "postgres".to_owned());
        let db_name = new_test_db_name(&base);

        let outcome = std::panic::AssertUnwindSafe(create_and_init(
            &options,
            &db_name,
            async |_options, _db_name| -> Result<PgPool, SetupError> { panic!("injected migration panic") },
        ))
        .catch_unwind()
        .await;

        let admin_pool = admin_pool_from_env(&options).await;
        let exists = database_exists(&admin_pool, &db_name).await;
        let cleanup = drop_test_database(&admin_pool, &db_name, true).await;
        admin_pool.close().await;
        cleanup.expect("regression cleanup");

        let panic = outcome.err().expect("the injected migration panic must propagate");
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"injected migration panic"),
            "the ORIGINAL panic payload must be resumed"
        );
        assert!(!exists, "the partially initialized database must be rolled back after a panic");
    }

    /// The custom unit-test runner must drop the database even when the
    /// scenario panics: cleanup runs BEFORE the original panic resumes.
    #[tokio::test]
    async fn runner_cleans_up_after_a_panicking_scenario() {
        let Some(_) = options_from_env() else { return };
        let created = Arc::new(Mutex::new(None::<String>));

        let outcome = std::panic::AssertUnwindSafe(run_with_test_db(async |db| {
            *created.lock().unwrap_or_else(|e| e.into_inner()) = Some(db.db_name.clone());
            panic!("simulated scenario panic");
        }))
        .catch_unwind()
        .await;
        let panic = outcome.expect_err("the scenario panic must propagate");
        assert_eq!(
            panic.downcast_ref::<&str>(),
            Some(&"simulated scenario panic"),
            "the ORIGINAL scenario panic payload must be resumed"
        );
        let name = created
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .expect("the scenario must have run");

        let options = connection_options().expect("DATABASE_URL must be present");
        let admin_pool = admin_pool_from_env(&options).await;
        let exists = database_exists(&admin_pool, &name).await;
        admin_pool.close().await;
        assert!(!exists, "the runner must drop the database after a scenario panic");
    }

    // ---- DB-backed sweep regression --------------------------------------

    /// Creates one legacy orphan, one aged current-format orphan and one
    /// fresh (still open) current-format database, sweeps, and verifies the
    /// orphans are gone while the fresh database survives. Cleans up after
    /// itself even when an assertion fails; any leftover is caught by the
    /// regular stale sweep on the next run.
    #[tokio::test]
    async fn sweep_removes_stale_orphans_and_keeps_fresh_parallel_databases() {
        let Some(options) = options_from_env() else { return };
        let base = options.get_database().map(str::to_owned).unwrap_or_else(|| "postgres".to_owned());
        let admin_pool = admin_pool_from_env(&options).await;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_secs();

        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let pid = std::process::id();
        let legacy = format!("{base}{TEST_SEPARATOR}{unique:032x}");
        // The stale/fresh names come from the PRODUCTION generator with a
        // controlled timestamp — this pins generator/parser agreement and
        // the length budget, not a hand-copied format.
        let stale = new_test_db_name_with(&base, now - STALE_AGE.as_secs() - 60, pid, &format!("{unique:016x}"));
        let fresh = new_test_db_name_with(&base, now, pid, &format!("{:016x}", unique + 1));
        for name in [&legacy, &stale, &fresh] {
            sqlx::query(&format!("CREATE DATABASE {}", quote_pg_identifier(name)))
                .execute(&admin_pool)
                .await
                .unwrap_or_else(|error| panic!("failed to create regression database '{name}': {error}"));
        }
        let fresh_pool = PgPoolOptions::new()
            .connect_with(options.database(&fresh))
            .await
            .expect("fresh regression database must be reachable");

        let outcome = async {
            let report = sweep_with_pool(&admin_pool, &base).await;
            if database_exists(&admin_pool, &legacy).await {
                return Err("legacy orphan still exists after sweep".into());
            }
            if database_exists(&admin_pool, &stale).await {
                return Err("aged orphan still exists after sweep".into());
            }
            if !database_exists(&admin_pool, &fresh).await {
                return Err("fresh parallel database was removed".into());
            }
            if report.deleted == 0 {
                return Err(format!("sweep reported no deletions at all: {report:?}"));
            }
            Ok(())
        }
        .await;

        // Cleanup regardless of the outcome.
        fresh_pool.close().await;
        let cleanup = drop_test_database(&admin_pool, &fresh, true).await;
        let _ = drop_test_database(&admin_pool, &legacy, false).await;
        let _ = drop_test_database(&admin_pool, &stale, false).await;
        admin_pool.close().await;
        cleanup.expect("fresh regression database cleanup");

        outcome.expect("sweep regression assertions");
    }

    async fn database_exists(admin_pool: &PgPool, name: &str) -> bool {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)")
            .bind(name)
            .fetch_one(admin_pool)
            .await
            .unwrap_or(false)
    }
}
