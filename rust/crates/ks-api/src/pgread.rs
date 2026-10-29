//! One read the way a Python tab reader makes it (`core/pg_*_read.fetch`):
//! the schema revision first — `core.pg.require_revision`, asked before every
//! read — then the statement with its parameters in order, both inside
//! asyncpg's `command_timeout`.

use std::time::Duration;

use chrono::NaiveDate;
use sqlx::postgres::PgRow;
use sqlx::{AssertSqlSafe, PgPool};

use crate::trend::ReadError;

/// A statement parameter, as the Python caller handed it to asyncpg.
#[derive(Clone, Debug)]
pub enum Bind {
    Date(NaiveDate),
    Text(String),
    /// A Python `int` where the statement wants an integer.
    Int(i64),
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
        let work = async {
            let found: Option<String> =
                sqlx::query_scalar("SELECT version_num FROM meta.alembic_version LIMIT 1")
                    .fetch_optional(&self.pool)
                    .await?;
            if found.as_deref() != Some(self.revision.as_str()) {
                return Err(sqlx::Error::Protocol(format!(
                    "schema is at {found:?}, this build requires {:?}",
                    self.revision
                )));
            }
            let mut query = sqlx::query(AssertSqlSafe(sql));
            for bind in binds {
                query = match bind {
                    Bind::Date(d) => query.bind(*d),
                    Bind::Text(t) => query.bind(t.clone()),
                    Bind::Int(n) => query.bind(*n),
                };
            }
            query.fetch_all(&self.pool).await
        };
        match tokio::time::timeout(self.timeout, work).await {
            Ok(Ok(rows)) => Ok(rows),
            Ok(Err(error)) => Err(ReadError(error.to_string())),
            Err(_) => Err(ReadError(format!("no answer within {:?}", self.timeout))),
        }
    }
}
