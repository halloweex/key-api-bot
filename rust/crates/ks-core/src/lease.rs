//! Whose job it is, and the lease that keeps two processes from running it at
//! once — the Rust half of К0's contract (plan, revision 4; `core/job_lease.py`
//! on the Python side, whose docstring is the contract's home).
//!
//! - **The switch**: `meta.chain_watermarks`, key `executor:<job_id>`. Its value
//!   exactly `rust` — byte for byte, no trim, no case — means Rust runs the
//!   job; any other value, or no row, means Python. Read before every run, by
//!   both sides. Neither side writes it: a person does
//!   (`scripts/job_executor.py set ch_sync rust|python`, or SQL). The role
//!   needs `SELECT` on that table and nothing else for it.
//! - **The lease**: a session-level `pg_try_advisory_lock($1::bigint)` on a
//!   connection of its own, held for the whole run, released with
//!   `pg_advisory_unlock($1::bigint)` at the end — or by the server when the
//!   session dies. Never the blocking form, nothing held between runs. A lease
//!   another process holds skips the run, and a skip is not an error. Taken
//!   only when the switch names Rust; Python takes it on every run while the
//!   switch names Python.
//! - **The key**: the bigint whose hex is `6b734a4c` followed by the first
//!   eight hex digits of sha256(name in UTF-8). Postgres computes the same:
//!   `('x' || '6b734a4c' || left(encode(sha256(convert_to($1,'UTF8')),'hex'),8))::bit(64)::bigint`.
//!   The prefix is the scheduler's jobs; a name with a `:` is Rust's own —
//!   `ch_sync:shadow`, the shadow's lease — and no scheduler job id carries one.
//! - **The holder names itself**: the lease session's `application_name` is
//!   `ks-rust-worker`, so Python's skip record (`lease_held`, with the holder's
//!   pid and application) says who has it.
//!
//! A lease is the cross-process answer to what `PG_LAYER_LOCK`, an asyncio
//! lock, answers inside the Python process: two writers of one staging table
//! interleaving `TRUNCATE` and `EXCHANGE` can swap an empty table into place
//! under a fresh watermark.

use std::future::Future;
use std::time::Duration;

use sha2::{Digest, Sha256};
use sqlx::postgres::{PgConnectOptions, PgConnection};
use sqlx::{Connection, Executor as _, PgPool};

use crate::pg::{APPLICATION_NAME, PgError, within};

/// Who runs a job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Executor {
    Python,
    Rust,
}

/// A switch or a lease that cannot be read.
#[derive(Debug)]
pub enum LeaseError {
    Pg(PgError),
}

impl std::fmt::Display for LeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseError::Pg(error) => write!(f, "lease: {error}"),
        }
    }
}

impl std::error::Error for LeaseError {}

impl From<PgError> for LeaseError {
    fn from(error: PgError) -> Self {
        LeaseError::Pg(error)
    }
}

/// Whose job a job is.
pub trait ExecutorSwitch: Send + Sync {
    fn executor(&self, job_id: &str) -> impl Future<Output = Result<Executor, LeaseError>> + Send;
}

/// The switch's statement and the value that names Rust.
pub const SWITCH_SQL: &str = "SELECT value FROM meta.chain_watermarks WHERE key = $1";
pub const RUST_VALUE: &str = "rust";

/// The switch's key for a job: `executor:<job_id>`.
pub fn switch_key(job_id: &str) -> String {
    format!("executor:{job_id}")
}

/// The switch as К0 keeps it, in `meta.chain_watermarks`.
#[derive(Clone, Debug)]
pub struct ChainSwitch {
    pool: PgPool,
    timeout: Duration,
}

impl ChainSwitch {
    pub fn new(pool: PgPool, timeout: Duration) -> Self {
        ChainSwitch { pool, timeout }
    }
}

impl ExecutorSwitch for ChainSwitch {
    async fn executor(&self, job_id: &str) -> Result<Executor, LeaseError> {
        let value: Option<String> = within(
            self.timeout,
            sqlx::query_scalar(SWITCH_SQL)
                .bind(switch_key(job_id))
                .fetch_optional(&self.pool),
        )
        .await?;
        Ok(match value.as_deref() {
            Some(RUST_VALUE) => Executor::Rust,
            _ => Executor::Python,
        })
    }
}

/// The lease's name: the job's id, and `<job_id>:shadow` for the shadow's
/// run, which writes only its own tables and so must exclude only another
/// shadow run.
pub fn lease_name(job_id: &str, shadow: bool) -> String {
    if shadow {
        format!("{job_id}:shadow")
    } else {
        job_id.to_owned()
    }
}

/// The scheduler's jobs' half of every key.
pub const KEY_PREFIX: u32 = 0x6b73_4a4c;

/// К0's key for a lease's name: `0x6b734a4c` above the first four bytes of
/// sha256(name). The prefix's top bit is clear, so the bigint is positive.
pub fn lease_key(name: &str) -> i64 {
    let digest = Sha256::digest(name.as_bytes());
    let low = u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]);
    (i64::from(KEY_PREFIX) << 32) | i64::from(low)
}

/// Something that grants leases.
pub trait JobLease: Send + Sync {
    /// The lease, or `None` when another process holds it.
    fn try_acquire(
        &self,
        name: &str,
    ) -> impl Future<Output = Result<Option<Lease>, LeaseError>> + Send;
}

pub const TRY_LOCK_SQL: &str = "SELECT pg_try_advisory_lock($1::bigint)";
pub const UNLOCK_SQL: &str = "SELECT pg_advisory_unlock($1::bigint)";

/// Leases as Postgres advisory locks, each on a connection of its own —
/// never a pooled one, which would carry the lock to whoever borrowed it
/// next — named `ks-rust-worker`.
#[derive(Clone, Debug)]
pub struct PgAdvisoryLease {
    options: PgConnectOptions,
    timeout: Duration,
}

impl PgAdvisoryLease {
    pub fn new(options: PgConnectOptions, timeout: Duration) -> Self {
        PgAdvisoryLease {
            options: options.application_name(APPLICATION_NAME),
            timeout,
        }
    }
}

impl JobLease for PgAdvisoryLease {
    async fn try_acquire(&self, name: &str) -> Result<Option<Lease>, LeaseError> {
        let key = lease_key(name);
        let mut conn = within(self.timeout, PgConnection::connect_with(&self.options)).await?;
        let granted: bool = within(
            self.timeout,
            sqlx::query_scalar(TRY_LOCK_SQL)
                .bind(key)
                .fetch_one(&mut conn),
        )
        .await?;
        if granted {
            Ok(Some(Lease {
                conn,
                name: name.to_owned(),
                key,
                timeout: self.timeout,
            }))
        } else {
            let _ = conn.close().await;
            Ok(None)
        }
    }
}

/// A held lease. Dropped without [`Lease::release`], its connection closes
/// and the server releases the lock with it.
#[derive(Debug)]
pub struct Lease {
    conn: PgConnection,
    name: String,
    key: i64,
    timeout: Duration,
}

impl Lease {
    /// The lease's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The advisory lock's key.
    pub fn key(&self) -> i64 {
        self.key
    }

    /// Whether the lease's connection still answers: a run that lost it is
    /// no longer the only one.
    pub async fn alive(&mut self) -> bool {
        within(self.timeout, self.conn.execute("SELECT 1"))
            .await
            .is_ok()
    }

    /// Unlock, then close the connection.
    pub async fn release(mut self) -> Result<(), LeaseError> {
        let unlocked = within(
            self.timeout,
            sqlx::query_scalar::<_, bool>(UNLOCK_SQL)
                .bind(self.key)
                .fetch_one(&mut self.conn),
        )
        .await;
        let _ = self.conn.close().await;
        unlocked.map(|_| ()).map_err(LeaseError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_keys_are_k0s_test_vectors() {
        assert_eq!(lease_key("ch_sync"), 7_742_613_876_423_907_721);
        assert_eq!(lease_key("ch_sync"), 0x6b73_4a4c_6941_d989);
        assert_eq!(lease_key("incremental_sync"), 7_742_613_875_706_779_467);
        assert_eq!(lease_key("memory_monitor"), 7_742_613_878_325_278_635);
        assert_eq!(lease_key("ch_sync:shadow"), 7_742_613_876_521_340_087);
        assert_eq!(lease_key("ch_sync:shadow"), 0x6b73_4a4c_6f10_8cb7);
    }

    #[test]
    fn the_names() {
        assert_eq!(lease_name("ch_sync", false), "ch_sync");
        assert_eq!(lease_name("ch_sync", true), "ch_sync:shadow");
        assert_eq!(switch_key("ch_sync"), "executor:ch_sync");
        assert_eq!(APPLICATION_NAME, "ks-rust-worker");
    }
}
