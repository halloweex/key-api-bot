//! `meta.mirror_state`, the per-table watermark — `core/ch_common.py`'s
//! `mark_ok` and `mark_failed`, statement for statement.
//!
//! The separate-transaction variant: the ClickHouse write a stamp reports on
//! has already happened in another store, so there is no data transaction to
//! share it with. A success resets `failures_since_ok` and clears
//! `last_error`; a failure counts and keeps the last success, so a row whose
//! two timestamps part says how long its table has been behind.
//!
//! The texts are Python's byte for byte; `contract/ch_sync.json` holds them,
//! and the contract test fails the day either side moves.

use std::time::Duration;

use sqlx::PgPool;

use crate::pg::{PgError, within};

/// `core.ch_common.mark_ok`'s statement.
pub const MARK_OK_SQL: &str = "
            INSERT INTO meta.mirror_state
                   (table_name, last_attempted_at, last_ok_at,
                    failures_since_ok, last_error, last_rows)
            VALUES ($1, now(), now(), 0, NULL, $2)
            ON CONFLICT (table_name) DO UPDATE SET
                last_attempted_at = now(), last_ok_at = now(),
                failures_since_ok = 0, last_error = NULL,
                last_rows = EXCLUDED.last_rows
            ";

/// `core.ch_common.mark_failed`'s statement — `pg_landing.RECORD_FAILURE_SQL`'s
/// text, at another indentation.
pub const MARK_FAILED_SQL: &str = "
                INSERT INTO meta.mirror_state
                       (table_name, last_attempted_at, failures_since_ok, last_error)
                VALUES ($1, now(), 1, $2)
                ON CONFLICT (table_name) DO UPDATE SET
                    last_attempted_at = now(),
                    failures_since_ok = meta.mirror_state.failures_since_ok + 1,
                    last_error = EXCLUDED.last_error
                ";

/// How much of an error is kept: Python's `error[:2000]`, in characters.
pub const ERROR_CHARS: usize = 2000;

/// A success: `rows` shipped to `state_row`'s table.
pub async fn mark_ok(
    pool: &PgPool,
    timeout: Duration,
    state_row: &str,
    rows: i64,
) -> Result<(), PgError> {
    within(
        timeout,
        sqlx::query(MARK_OK_SQL)
            .bind(state_row)
            .bind(rows)
            .execute(pool),
    )
    .await
    .map(|_| ())
}

/// A failure, best effort: `pg_landing._record_failure`'s reasoning. The case
/// that misleads is ClickHouse failing while Postgres is fine, and without
/// this the row would keep its last success and read as healthy. If
/// Postgres is what failed, the stamp fails too, and that is logged.
pub async fn mark_failed(pool: &PgPool, timeout: Duration, state_row: &str, error: &str) {
    let kept: String = error.chars().take(ERROR_CHARS).collect();
    let stamped = within(
        timeout,
        sqlx::query(MARK_FAILED_SQL)
            .bind(state_row)
            .bind(kept)
            .execute(pool),
    )
    .await;
    if let Err(stamp_error) = stamped {
        tracing::warn!(
            state_row,
            "could not record the failure either: {stamp_error}"
        );
    }
}
