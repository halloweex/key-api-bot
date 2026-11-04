//! The three things access reads, in the stores production keeps them in:
//! `KS_USER_STORE=postgres` and `KS_BOT_STORE=postgres`, which the service
//! requires at start.
//!
//! The dashboard's user list and the role matrix are read the way
//! `core/pg_dashboard_users.py` reads them: the schema revision first. The
//! bot's list is read the way `bot/store_postgres.py` reads it — no revision
//! check, the bot store checks it once at `initialise()`.
//!
//! None of their failures is answered here. Python refuses a request whose
//! user list it cannot read, 500s on an unreadable bot list and falls back to
//! the code's matrix — and in a migration window its own reads succeed where
//! these fail — so a failure hands the request to Python, which answers it as
//! it would have (`crate::handover`).

use std::future::Future;
use std::time::Duration;

use sqlx::PgPool;

use super::permissions::{Actions, StoredPermission};
use crate::handover::{self, Reason};
use crate::pgread;
use crate::trend::ReadError;

/// The columns of `app.dashboard_users` a session needs.
#[derive(Clone, Debug)]
pub struct UserRow {
    pub role: String,
    pub status: String,
    pub allowed_features: Option<String>,
}

/// A read the session needed and did not get, and why the request is handed over.
#[derive(Debug)]
pub struct StoreError {
    pub reason: Reason,
    pub message: String,
}

impl From<sqlx::Error> for StoreError {
    fn from(error: sqlx::Error) -> Self {
        StoreError {
            reason: handover::of(&error, Reason::Session),
            message: error.to_string(),
        }
    }
}

impl From<ReadError> for StoreError {
    fn from(error: ReadError) -> Self {
        match error {
            ReadError::HandOver(reason, message) => StoreError { reason, message },
            ReadError::Failed(message) => StoreError {
                reason: Reason::Session,
                message,
            },
        }
    }
}

pub trait AccessStore: Sync {
    /// `store.get_user(user_id)`.
    fn dashboard_user(
        &self,
        user_id: i64,
    ) -> impl Future<Output = Result<Option<UserRow>, StoreError>> + Send;

    /// `get_bot_store().access.status(user_id)`: no row, or the row's status.
    fn bot_status(
        &self,
        user_id: i64,
    ) -> impl Future<Output = Result<Option<Option<String>>, StoreError>> + Send;

    /// `store.get_role_permissions(role)`.
    fn role_permissions(
        &self,
        role: &str,
    ) -> impl Future<Output = Result<Vec<StoredPermission>, StoreError>> + Send;
}

pub struct PgAccess {
    pool: PgPool,
    timeout: Duration,
    revision: String,
}

impl PgAccess {
    pub fn new(pool: PgPool, timeout: Duration, revision: String) -> Self {
        Self {
            pool,
            timeout,
            revision,
        }
    }

    async fn within<T>(
        &self,
        work: impl Future<Output = Result<T, StoreError>>,
    ) -> Result<T, StoreError> {
        tokio::time::timeout(self.timeout, work)
            .await
            .map_err(|_| StoreError {
                reason: Reason::Session,
                message: format!("no answer within {:?}", self.timeout),
            })?
    }

    /// Whether this service can answer anybody: Postgres answers its role,
    /// and the schema is at the revision the contract was exported from. An
    /// image behind the schema hands every request to Python, so `/readyz` —
    /// the container's healthcheck — asks exactly this.
    pub async fn ready(&self) -> Result<(), StoreError> {
        pgread::connection(&self.pool, &self.revision, self.timeout).await?;
        Ok(())
    }
}

impl AccessStore for PgAccess {
    async fn dashboard_user(&self, user_id: i64) -> Result<Option<UserRow>, StoreError> {
        self.within(async {
            let mut conn = pgread::connection(&self.pool, &self.revision, self.timeout).await?;
            let row: Option<(String, String, Option<String>)> = sqlx::query_as(
                "SELECT role, status, allowed_features FROM app.dashboard_users WHERE user_id = $1",
            )
            .bind(user_id)
            .fetch_optional(&mut *conn)
            .await?;
            Ok(row.map(|(role, status, allowed_features)| UserRow {
                role,
                status,
                allowed_features,
            }))
        })
        .await
    }

    async fn bot_status(&self, user_id: i64) -> Result<Option<Option<String>>, StoreError> {
        self.within(async {
            let row: Option<(Option<String>,)> =
                sqlx::query_as("SELECT status FROM app.authorized_users WHERE user_id = $1")
                    .bind(user_id)
                    .fetch_optional(&self.pool)
                    .await?;
            Ok(row.map(|(status,)| status))
        })
        .await
    }

    async fn role_permissions(&self, role: &str) -> Result<Vec<StoredPermission>, StoreError> {
        self.within(async {
            let mut conn = pgread::connection(&self.pool, &self.revision, self.timeout).await?;
            let rows: Vec<(String, bool, bool, bool)> = sqlx::query_as(
                "SELECT feature, can_view, can_edit, can_delete \
                 FROM app.role_permissions WHERE role = $1",
            )
            .bind(role)
            .fetch_all(&mut *conn)
            .await?;
            Ok(rows
                .into_iter()
                .map(|(feature, view, edit, delete)| StoredPermission {
                    feature,
                    actions: Actions { view, edit, delete },
                })
                .collect())
        })
        .await
    }
}
