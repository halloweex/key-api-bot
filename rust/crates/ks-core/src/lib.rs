//! The core's shared base (plan, revision 4, К1): what every Rust job of the
//! core reads and writes through. Exactly what the ClickHouse copy needs —
//! the first job ported — and nothing a later component has not asked for.
//!
//! - [`pg`]: Postgres through sqlx, under the names and defaults `core/pg.py`
//!   gives asyncpg (`KS_PG_DSN`, `KS_PG_POOL_MIN`, `KS_PG_POOL_MAX`,
//!   `KS_PG_TIMEOUT`).
//! - [`ch`]: ClickHouse over HTTP, credentials in headers
//!   (`core/ch_common.py`'s `execute` and `ensure_database`).
//! - [`tsv`]: ClickHouse's TabSeparated, both ways, typed per table.
//! - [`money`]: hryvnia as a decimal, never a float.
//! - [`kyiv`]: the Kyiv calendar date of an instant.
//! - [`warehouse`]: Silver's column order and Gold's measures — the one text
//!   every engine renders (`core/warehouse_sql.py`, `core/pg_gold.py`).
//! - [`watermark`]: `meta.mirror_state`, the two statements
//!   `core/ch_common.py` runs.
//! - [`alert`]: where a job's finding goes. An interface: nothing here
//!   reaches the alert journal or Telegram.
//! - [`lease`]: whose job it is, and the lease that keeps two processes from
//!   running it at once. An interface: the Python side (К0) defines both.
//! - [`config`]: environment variables, read as Python reads them.

pub mod alert;
pub mod ch;
pub mod config;
pub mod kyiv;
pub mod lease;
pub mod money;
pub mod pg;
pub mod tsv;
pub mod warehouse;
pub mod watermark;
