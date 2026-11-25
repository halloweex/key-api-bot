//! Whose job it is, and the lease that keeps two processes from running it at
//! once (plan, revision 4, К0).
//!
//! The Python side defines both — its pull request is the contract — so this
//! is the interface the Rust worker is written against, with the stand-ins
//! it runs on until that pull request exists:
//!
//! - [`ExecutorSwitch`]: whose job a job is, per job id. [`EnvSwitch`] reads
//!   `KS_EXECUTOR_<JOB>` (`KS_EXECUTOR_CH_SYNC=rust`); unset is Python's.
//! - [`JobLease`]: a session-level advisory lock taken on a connection of its
//!   own and held for the whole run — released at the end, and by the server
//!   when the process dies. [`PgAdvisoryLease`] derives the lock's key in
//!   Postgres (`hashtextextended(name, 0)`), so any process that asks for a
//!   name, in any language, asks for the same lock.
//!
//! A lease is the cross-process answer to what `PG_LAYER_LOCK`, an asyncio
//! lock, answers inside the Python process: two writers of one staging table
//! interleaving `TRUNCATE` and `EXCHANGE` can swap an empty table into place
//! under a fresh watermark.

use std::future::Future;
use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgConnection};
use sqlx::{Connection, Executor as _};

use crate::config;
use crate::pg::{PgError, within};

/// Who runs a job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Executor {
    Python,
    Rust,
}

/// A setting or a lock that cannot be read.
#[derive(Debug)]
pub enum LeaseError {
    Config(String),
    Pg(PgError),
}

impl std::fmt::Display for LeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeaseError::Config(text) => f.write_str(text),
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

/// The stand-in switch: `KS_EXECUTOR_<JOB ID IN CAPITALS>`, `python` or
/// `rust`; unset is `python`, anything else an error, which the worker reads
/// as not its job.
#[derive(Clone, Copy, Debug, Default)]
pub struct EnvSwitch;

impl EnvSwitch {
    /// The variable that names `job_id`'s executor.
    pub fn variable(job_id: &str) -> String {
        format!("KS_EXECUTOR_{}", job_id.to_ascii_uppercase())
    }
}

impl ExecutorSwitch for EnvSwitch {
    fn executor(&self, job_id: &str) -> impl Future<Output = Result<Executor, LeaseError>> + Send {
        let variable = Self::variable(job_id);
        let answer = match config::trimmed(&variable).as_deref() {
            None | Some("python") => Ok(Executor::Python),
            Some("rust") => Ok(Executor::Rust),
            Some(_) => Err(LeaseError::Config(format!(
                "{variable} must be python or rust"
            ))),
        };
        std::future::ready(answer)
    }
}

/// The lease's name for a job: `ks.job:<id>`, and `ks.job:<id>:shadow` for
/// the shadow's run, which writes only its own tables and so must exclude
/// only another shadow run.
pub fn lease_name(job_id: &str, shadow: bool) -> String {
    if shadow {
        format!("ks.job:{job_id}:shadow")
    } else {
        format!("ks.job:{job_id}")
    }
}

/// Something that grants leases.
pub trait JobLease: Send + Sync {
    /// The lease, or `None` when another process holds it.
    fn try_acquire(
        &self,
        name: &str,
    ) -> impl Future<Output = Result<Option<Lease>, LeaseError>> + Send;
}

/// `pg_try_advisory_lock`, never the blocking form: a job that finds its
/// lease taken skips the tick, as `max_instances=1` makes APScheduler skip.
pub const TRY_LOCK_SQL: &str = "SELECT pg_try_advisory_lock(hashtextextended($1, 0))";
pub const UNLOCK_SQL: &str = "SELECT pg_advisory_unlock(hashtextextended($1, 0))";

/// Leases as Postgres advisory locks, each on a connection of its own —
/// never a pooled one, which would carry the lock to whoever borrowed it next.
#[derive(Clone, Debug)]
pub struct PgAdvisoryLease {
    options: PgConnectOptions,
    timeout: Duration,
}

impl PgAdvisoryLease {
    pub fn new(options: PgConnectOptions, timeout: Duration) -> Self {
        PgAdvisoryLease { options, timeout }
    }
}

impl JobLease for PgAdvisoryLease {
    async fn try_acquire(&self, name: &str) -> Result<Option<Lease>, LeaseError> {
        let mut conn = within(self.timeout, PgConnection::connect_with(&self.options)).await?;
        let granted: bool = within(
            self.timeout,
            sqlx::query_scalar(TRY_LOCK_SQL)
                .bind(name)
                .fetch_one(&mut conn),
        )
        .await?;
        if granted {
            Ok(Some(Lease {
                conn,
                name: name.to_owned(),
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
    timeout: Duration,
}

impl Lease {
    /// The lease's name.
    pub fn name(&self) -> &str {
        &self.name
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
                .bind(&self.name)
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
    fn the_names() {
        assert_eq!(lease_name("ch_sync", false), "ks.job:ch_sync");
        assert_eq!(lease_name("ch_sync", true), "ks.job:ch_sync:shadow");
        assert_eq!(EnvSwitch::variable("ch_sync"), "KS_EXECUTOR_CH_SYNC");
    }
}
