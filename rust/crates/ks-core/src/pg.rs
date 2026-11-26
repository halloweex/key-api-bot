//! Postgres through sqlx, configured as `core/pg.py` configures asyncpg:
//! `KS_PG_DSN`, a small pool (`KS_PG_POOL_MIN` 1, `KS_PG_POOL_MAX` 5 — the
//! server is shared and small), and `KS_PG_TIMEOUT` (30 s) on every call —
//! asyncpg's `command_timeout`, here a timeout around each statement.
//!
//! The pool opens lazily, as `get_pool()` does: a worker that never reaches
//! Postgres never connects. Its connections carry `application_name`
//! `ks-rust-worker`, so `pg_stat_activity` tells the Rust worker from the Python
//! app.

use std::future::Future;
use std::str::FromStr;
use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use crate::config;

pub const DSN_ENV: &str = "KS_PG_DSN";
pub const APPLICATION_NAME: &str = "ks-rust-worker";

/// What a Postgres call can fail with.
#[derive(Debug)]
pub enum PgError {
    /// The settings cannot be used. Never carries the address.
    Config(String),
    /// No answer within the timeout.
    Timeout(Duration),
    /// The statement failed.
    Sql(sqlx::Error),
}

impl std::fmt::Display for PgError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PgError::Config(text) => f.write_str(text),
            PgError::Timeout(after) => {
                write!(f, "Postgres: no answer within {} s", after.as_secs_f64())
            }
            PgError::Sql(error) => write!(f, "Postgres: {error}"),
        }
    }
}

impl std::error::Error for PgError {}

impl From<sqlx::Error> for PgError {
    fn from(error: sqlx::Error) -> Self {
        PgError::Sql(error)
    }
}

/// Where Postgres is and how patient to be with it.
#[derive(Clone, Debug)]
pub struct PgSettings {
    pub options: PgConnectOptions,
    pub pool_min: u32,
    pub pool_max: u32,
    pub timeout: Duration,
}

impl PgSettings {
    /// The settings `core/pg.py` reads, under its names and defaults.
    pub fn from_env() -> Result<Self, PgError> {
        let dsn = config::trimmed(DSN_ENV).ok_or_else(|| {
            PgError::Config(format!(
                "{DSN_ENV} is not set — refusing to guess a database. \
                 Expected postgresql://ks_app:...@postgres:5432/ks"
            ))
        })?;
        let options = PgConnectOptions::from_str(&dsn)
            .map_err(|_| PgError::Config(format!("{DSN_ENV} is not a Postgres address")))?
            .application_name(APPLICATION_NAME);
        let setting = |e: config::ConfigError| PgError::Config(e.0);
        Ok(PgSettings {
            options,
            pool_min: config::count("KS_PG_POOL_MIN", 1).map_err(setting)?,
            pool_max: config::count("KS_PG_POOL_MAX", 5).map_err(setting)?,
            timeout: config::seconds("KS_PG_TIMEOUT", 30.0).map_err(setting)?,
        })
    }

    /// The pool, opened on first use.
    pub fn pool(&self) -> PgPool {
        PgPoolOptions::new()
            .min_connections(self.pool_min)
            .max_connections(self.pool_max.max(1))
            .acquire_timeout(self.timeout)
            .connect_lazy_with(self.options.clone())
    }
}

/// `call`, answered within `timeout`.
pub async fn within<T, F>(timeout: Duration, call: F) -> Result<T, PgError>
where
    F: Future<Output = Result<T, sqlx::Error>>,
{
    match tokio::time::timeout(timeout, call).await {
        Ok(result) => result.map_err(PgError::Sql),
        Err(_) => Err(PgError::Timeout(timeout)),
    }
}
