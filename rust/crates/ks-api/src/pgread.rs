//! One read the way a Python tab reader makes it (`core/pg_*_read.fetch`):
//! the schema revision first — `core.pg.require_revision`, asked before every
//! read — then the statement with its parameters in order, inside asyncpg's
//! `command_timeout`.
//!
//! Everything before the statement hands the request to the Python app
//! rather than answer it: no connection within the pool's wait, a revision
//! check Postgres did not answer, the schema at another revision. Python's
//! own reader may well answer then — its pool is its own, and in a migration
//! window its image is the schema's — so a 503 or a 401 from here would be
//! the pilot's failure, not the data's. A statement that fails is the data's:
//! the 503 Python answers under `KS_READ_FALLBACK=off`.

use std::future::Future;
use std::time::Duration;

use chrono::NaiveDate;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgConnection, PgRow};
use sqlx::{AssertSqlSafe, PgPool, Postgres};

use crate::handover::{self, Reason};
use crate::trend::ReadError;

/// A statement parameter, as the Python caller handed it to asyncpg.
#[derive(Clone, Debug)]
pub enum Bind {
    Date(NaiveDate),
    Text(String),
    /// A Python `int` where the statement wants an integer.
    Int(i64),
}

/// `require_revision()` on `conn`: the schema at `revision`, or a hand-over.
pub async fn require_revision(conn: &mut PgConnection, revision: &str) -> Result<(), ReadError> {
    let found: Option<String> =
        sqlx::query_scalar("SELECT version_num FROM meta.alembic_version LIMIT 1")
            .fetch_optional(&mut *conn)
            .await
            .map_err(|e| ReadError::HandOver(handover::of(&e, Reason::Database), e.to_string()))?;
    if found.as_deref() == Some(revision) {
        Ok(())
    } else {
        Err(ReadError::HandOver(
            Reason::Revision,
            format!("schema is at {found:?}, this build requires {revision:?}"),
        ))
    }
}

/// A connection for one read, the revision checked on it within `timeout`.
pub async fn connection(
    pool: &PgPool,
    revision: &str,
    timeout: Duration,
) -> Result<PoolConnection<Postgres>, ReadError> {
    let mut conn = pool
        .acquire()
        .await
        .map_err(|e| ReadError::HandOver(handover::of(&e, Reason::Database), e.to_string()))?;
    tokio::time::timeout(timeout, require_revision(&mut conn, revision))
        .await
        .map_err(|_| {
            ReadError::HandOver(
                Reason::Database,
                format!("the revision check had no answer within {timeout:?}"),
            )
        })??;
    Ok(conn)
}

/// The statement, within `timeout`: a failure here is the data's.
pub async fn fetch(
    statement: impl Future<Output = Result<Vec<PgRow>, sqlx::Error>>,
    timeout: Duration,
) -> Result<Vec<PgRow>, ReadError> {
    tokio::time::timeout(timeout, statement)
        .await
        .map_err(|_| ReadError::Failed(format!("no answer within {timeout:?}")))?
        .map_err(|e| ReadError::Failed(e.to_string()))
}

pub struct PgReads {
    pool: PgPool,
    /// asyncpg's `command_timeout` (`KS_PG_TIMEOUT`, 30 s).
    timeout: Duration,
    /// `REQUIRED_REVISION`.
    revision: String,
}

impl PgReads {
    pub fn new(pool: PgPool, timeout: Duration, revision: String) -> Self {
        Self {
            pool,
            timeout,
            revision,
        }
    }

    pub async fn rows(&self, sql: String, binds: &[Bind]) -> Result<Vec<PgRow>, ReadError> {
        let mut conn = connection(&self.pool, &self.revision, self.timeout).await?;
        let mut query = sqlx::query(AssertSqlSafe(sql));
        for bind in binds {
            query = match bind {
                Bind::Date(d) => query.bind(*d),
                Bind::Text(t) => query.bind(t.clone()),
                Bind::Int(n) => query.bind(*n),
            };
        }
        fetch(query.fetch_all(&mut *conn), self.timeout).await
    }
}
