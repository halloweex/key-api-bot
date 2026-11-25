//! `ch_sync`, the hourly ClickHouse copy — `core/ch_silver.py`'s
//! `ch_silver_sync` and `core/ch_history.py`'s `ship_history`, in
//! `Scheduler._run_ch_sync`'s order, statement for statement.
//!
//! 1. Silver is read from Postgres in one REPEATABLE READ snapshot and
//!    shipped whole into a staging table, swapped in by one
//!    `EXCHANGE TABLES` only when the staged count matches what Postgres
//!    supplied — a reader never sees a half-shipped table.
//! 2. Gold is then derived inside ClickHouse from the Silver just shipped,
//!    both grains, from `GOLD_MEASURES` rendered verbatim ([`ks_core::warehouse`]).
//! 3. The archive of order versions is appended by an ids-diff: every
//!    version Postgres holds that the copy lacks, no cursor, so a version
//!    lost below the copy's own high-water mark heals within the hour.
//! 4. Each step stamps its row in `meta.mirror_state`; a failure counts
//!    against the row of the step that failed, Silver's until Silver is
//!    stamped, Gold's after.
//!
//! **Live and shadow.** [`Target::Live`] writes the tables and state rows
//! Python writes. [`Target::Shadow`] writes its own: the same names with
//! `_rust` (`silver.orders_rust`, its `_rust_staging`, …) in the same
//! ClickHouse databases, and state rows prefixed `rust_shadow.`, which no
//! reader of `meta.mirror_state` asks for — every reader asks by name.
//!
//! The one thing Python does here that this does not is take
//! `PG_LAYER_LOCK`, an asyncio lock no other process can share. Its two
//! jobs are done otherwise: the snapshot keeps the Silver that is shipped
//! whole whatever the derivation commits meanwhile, and the lease
//! ([`ks_core::lease`]) keeps two writers off one staging table.

use std::collections::HashSet;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, Timelike, Utc};
use ks_core::alert::{AlertSink, Finding, Severity};
use ks_core::ch::{ChError, ClickHouse};
use ks_core::money::Money;
use ks_core::pg::{PgError, within};
use ks_core::tsv::{self, Cell, Column, Kind, TsvError};
use ks_core::{config, warehouse, watermark};
use rust_decimal::Decimal;
use serde_json::{Map, Value, json};
use sqlx::postgres::PgRow;
use sqlx::{AssertSqlSafe, PgPool, Row};

pub const JOB_ID: &str = "ch_sync";
/// The archive's database: `core.ch_history.DB_ENV`.
pub const HISTORY_DB_ENV: &str = "KS_CH_HISTORY_DB";
/// Versions per archive INSERT: `ship_history`'s default `chunk`.
pub const HISTORY_CHUNK: usize = 50_000;
/// The snapshot Silver is read in.
pub const SNAPSHOT: &str = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY";
/// What the shadow appends to every table name.
pub const SHADOW_SUFFIX: &str = "_rust";
/// What the shadow puts before every `meta.mirror_state` row it writes.
pub const SHADOW_STATE_PREFIX: &str = "rust_shadow.";

const fn column(name: &'static str, kind: Kind) -> Column {
    Column { name, kind }
}

/// `core.ch_silver._TYPES` over `SILVER_COLUMNS`.
pub const SILVER_COLUMNS: [Column; 15] = [
    column("id", Kind::Int32),
    column("source_id", Kind::Int32),
    column("status_id", Kind::Int32),
    column("grand_total", Kind::Decimal),
    column("ordered_at", Kind::Ts),
    column("buyer_id", Kind::Int32),
    column("manager_id", Kind::Int32),
    column("order_date", Kind::Date),
    column("is_return", Kind::Bool),
    column("sales_type", Kind::Str),
    column("is_active_source", Kind::Bool),
    column("source_name", Kind::Str),
    column("is_new_customer", Kind::Bool),
    column("buyer_first_order_date", Kind::Date),
    column("promocode", Kind::Str),
];

/// `core.ch_history.COLUMNS` with `_TYPES`.
pub const HISTORY_COLUMNS: [Column; 14] = [
    column("id", Kind::Int64),
    column("order_id", Kind::Int32),
    column("captured_at", Kind::Ts6),
    column("kind", Kind::Str),
    column("source_id", Kind::Int32),
    column("status_id", Kind::Int32),
    column("status_group_id", Kind::Int32),
    column("grand_total", Kind::Decimal),
    column("ordered_at", Kind::Ts6),
    column("buyer_id", Kind::Int32),
    column("manager_id", Kind::Int32),
    column("manager_comment", Kind::Str),
    column("promocode", Kind::Str),
    column("updated_at", Kind::Ts6),
];

/// `core.ch_silver._SILVER_DDL`.
pub const SILVER_DDL: &str = "
CREATE TABLE IF NOT EXISTS {name} (
    id                     Int32,
    source_id              Int32,
    status_id              Int32,
    grand_total            Decimal(12, 2),
    ordered_at             Nullable(DateTime('UTC')),
    buyer_id               Nullable(Int32),
    manager_id             Nullable(Int32),
    order_date             Date,
    is_return              Bool,
    sales_type             LowCardinality(String),
    is_active_source       Bool,
    source_name            String,
    is_new_customer        Bool,
    buyer_first_order_date Nullable(Date),
    promocode              Nullable(String)
)
ENGINE = MergeTree
ORDER BY (order_date, sales_type, id)
";

/// `core.ch_gold._TABLE_DDL`.
pub const GOLD_DDL: &str = "
CREATE TABLE IF NOT EXISTS {name} (
    date                Date,
    sales_type          LowCardinality(String),
    source_id           Nullable(Int32),
    revenue             Decimal(14, 2),
    orders_count        Int32,
    unique_customers    Int32,
    new_customers       Int32,
    returning_customers Int32,
    returns_count       Int32,
    returns_revenue     Decimal(14, 2),
    avg_order_value     Decimal(12, 2)
)
ENGINE = MergeTree
ORDER BY (date, sales_type)
";

/// `core.ch_history._DDL`.
pub const HISTORY_DDL: &str = "
CREATE TABLE IF NOT EXISTS {name} (
    id              Int64,
    order_id        Int32,
    captured_at     DateTime64(6, 'UTC'),
    kind            LowCardinality(String),
    source_id       Nullable(Int32),
    status_id       Nullable(Int32),
    status_group_id Nullable(Int32),
    grand_total     Nullable(Decimal(12, 2)),
    ordered_at      Nullable(DateTime64(6, 'UTC')),
    buyer_id        Nullable(Int32),
    manager_id      Nullable(Int32),
    manager_comment Nullable(String),
    promocode       Nullable(String),
    updated_at      Nullable(DateTime64(6, 'UTC'))
)
ENGINE = MergeTree
ORDER BY (order_id, captured_at, id)
";

/// A DDL template with its table named: Python's `.format(name=...)`.
pub fn ddl(template: &str, name: &str) -> String {
    template.replace("{name}", name)
}

/// Which tables a run writes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    /// Python's tables and state rows: only when the switch names Rust.
    Live,
    /// The shadow's own.
    Shadow,
}

/// Every name a run writes to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Names {
    pub silver: String,
    pub silver_staging: String,
    pub gold: String,
    pub gold_staging: String,
    pub history_db: String,
    pub history: String,
    pub silver_state: String,
    pub gold_state: String,
    pub history_state: String,
}

impl Names {
    pub fn new(target: Target, history_db: &str) -> Names {
        let (suffix, prefix) = match target {
            Target::Live => ("", ""),
            Target::Shadow => (SHADOW_SUFFIX, SHADOW_STATE_PREFIX),
        };
        Names {
            silver: format!("silver.orders{suffix}"),
            silver_staging: format!("silver.orders{suffix}_staging"),
            gold: format!("gold.daily_revenue{suffix}"),
            gold_staging: format!("gold.daily_revenue{suffix}_staging"),
            history_db: history_db.to_owned(),
            history: format!("{history_db}.order_versions{suffix}"),
            silver_state: format!("{prefix}clickhouse.silver_orders"),
            gold_state: format!("{prefix}clickhouse.gold_daily_revenue"),
            history_state: format!("{prefix}clickhouse.order_versions"),
        }
    }
}

/// `KS_CH_HISTORY_DB`, `history` when unset: `core.ch_history.table()`.
pub fn history_db() -> String {
    config::trimmed(HISTORY_DB_ENV).unwrap_or_else(|| "history".to_owned())
}

/// `core.ch_silver.derive_gold_sql`: the fine grain's INSERT and the roll-up's.
pub fn derive_gold_sql(names: &Names) -> (String, String) {
    let items = warehouse::gold_measures()
        .iter()
        .map(|(name, expression)| format!("{expression} AS {name}"))
        .collect::<Vec<_>>()
        .join(",\n    ");
    let columns = warehouse::gold_columns().join(", ");
    let (staging, silver) = (&names.gold_staging, &names.silver);
    let fine = format!(
        "\nINSERT INTO {staging}\n    ({columns})\nSELECT order_date AS date, sales_type, source_id,\n    {items}\nFROM {silver}\nGROUP BY order_date, sales_type, source_id\n"
    );
    let rollup = format!(
        "\nINSERT INTO {staging}\n    ({columns})\nSELECT order_date AS date, sales_type, NULL,\n    {items}\nFROM {silver}\nGROUP BY order_date, sales_type\n"
    );
    (fine, rollup)
}

/// What a step can fail with. Its text is what `meta.mirror_state` keeps.
#[derive(Debug)]
pub enum SyncError {
    Pg(PgError),
    Ch(ChError),
    Tsv(TsvError),
    Short { staged: i64, supplied: usize },
    Answer(String),
}

impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncError::Pg(error) => error.fmt(f),
            SyncError::Ch(error) => error.fmt(f),
            SyncError::Tsv(error) => write!(f, "TabSeparated: {error}"),
            SyncError::Short { staged, supplied } => write!(
                f,
                "staging holds {staged} row(s), Postgres supplied {supplied} — \
                 refusing to EXCHANGE a short copy"
            ),
            SyncError::Answer(text) => write!(f, "ClickHouse answered {text:?}, not a count"),
        }
    }
}

impl std::error::Error for SyncError {}

impl From<PgError> for SyncError {
    fn from(error: PgError) -> Self {
        SyncError::Pg(error)
    }
}

impl From<ChError> for SyncError {
    fn from(error: ChError) -> Self {
        SyncError::Ch(error)
    }
}

impl From<TsvError> for SyncError {
    fn from(error: TsvError) -> Self {
        SyncError::Tsv(error)
    }
}

impl From<sqlx::Error> for SyncError {
    fn from(error: sqlx::Error) -> Self {
        SyncError::Pg(PgError::Sql(error))
    }
}

/// `int(text.strip() or 0)`.
fn number(text: &str) -> Result<i64, SyncError> {
    let text = text.trim();
    if text.is_empty() {
        return Ok(0);
    }
    text.parse()
        .map_err(|_| SyncError::Answer(text.chars().take(100).collect()))
}

fn rows_i64(rows: usize) -> i64 {
    i64::try_from(rows).unwrap_or(i64::MAX)
}

/// A Postgres row as cells, by its columns' kinds.
fn decode(row: &PgRow, columns: &[Column]) -> Result<Vec<Cell>, sqlx::Error> {
    columns
        .iter()
        .enumerate()
        .map(|(i, column)| {
            Ok(match column.kind {
                Kind::Int32 => row
                    .try_get::<Option<i32>, _>(i)?
                    .map_or(Cell::Null, |v| Cell::Int(i64::from(v))),
                Kind::Int64 => row
                    .try_get::<Option<i64>, _>(i)?
                    .map_or(Cell::Null, Cell::Int),
                Kind::Decimal => row
                    .try_get::<Option<Decimal>, _>(i)?
                    .map_or(Cell::Null, |v| Cell::Money(Money::new(v))),
                Kind::Str => row
                    .try_get::<Option<String>, _>(i)?
                    .map_or(Cell::Null, Cell::Str),
                Kind::Date => row
                    .try_get::<Option<NaiveDate>, _>(i)?
                    .map_or(Cell::Null, Cell::Date),
                Kind::Bool => row
                    .try_get::<Option<bool>, _>(i)?
                    .map_or(Cell::Null, Cell::Bool),
                // `_fetch_pg_silver` truncates to the second, as the DDL holds.
                Kind::Ts => row
                    .try_get::<Option<DateTime<Utc>>, _>(i)?
                    .map_or(Cell::Null, |at| {
                        Cell::Time(at.with_nanosecond(0).unwrap_or(at))
                    }),
                Kind::Ts6 => row
                    .try_get::<Option<DateTime<Utc>>, _>(i)?
                    .map_or(Cell::Null, Cell::Time),
            })
        })
        .collect()
}

#[derive(Clone, Copy)]
enum Stage {
    Silver,
    Gold,
}

/// One run of the job.
pub struct ChSync<'a> {
    pub pg: &'a PgPool,
    pub timeout: Duration,
    pub ch: &'a ClickHouse,
    pub names: Names,
    pub history_chunk: usize,
    pub alerts: &'a dyn AlertSink,
}

impl ChSync<'_> {
    /// `_run_ch_sync`: Silver and Gold, then the archive. Never fails: each
    /// half answers its own error, as in Python, in Python's keys —
    /// `silver_rows`, `gold_rows` or `error`, and `history`.
    pub async fn run(&self) -> Value {
        let mut result = self.silver_and_gold().await;
        result.insert("history".into(), self.history().await);
        Value::Object(result)
    }

    async fn silver_and_gold(&self) -> Map<String, Value> {
        let mut stage = Stage::Silver;
        let mut answer = Map::new();
        match self.ship_and_derive(&mut stage).await {
            Ok((silver_rows, gold_rows)) => {
                tracing::info!(
                    "ch_silver: shipped {silver_rows} silver row(s) to {}, derived {gold_rows} gold row(s)",
                    self.names.silver
                );
                answer.insert("silver_rows".into(), json!(silver_rows));
                answer.insert("gold_rows".into(), json!(gold_rows));
            }
            Err(error) => {
                let detail = error.to_string();
                tracing::error!("ch_silver: sync failed: {detail}");
                let row = match stage {
                    Stage::Silver => &self.names.silver_state,
                    Stage::Gold => &self.names.gold_state,
                };
                watermark::mark_failed(self.pg, self.timeout, row, &detail).await;
                answer.insert("error".into(), json!(detail));
            }
        }
        answer
    }

    async fn ship_and_derive(&self, stage: &mut Stage) -> Result<(usize, i64), SyncError> {
        let rows = self.fetch_silver().await?;
        self.ship_silver(&rows).await?;
        watermark::mark_ok(
            self.pg,
            self.timeout,
            &self.names.silver_state,
            rows_i64(rows.len()),
        )
        .await?;
        *stage = Stage::Gold;
        let gold_rows = self.derive_gold().await?;
        watermark::mark_ok(self.pg, self.timeout, &self.names.gold_state, gold_rows).await?;
        Ok((rows.len(), gold_rows))
    }

    /// `_fetch_pg_silver`, in one snapshot.
    async fn fetch_silver(&self) -> Result<Vec<Vec<Cell>>, SyncError> {
        let select = format!(
            "SELECT {} FROM silver.orders",
            warehouse::SILVER_COLUMNS.join(", ")
        );
        let mut snapshot = within(self.timeout, self.pg.begin_with(SNAPSHOT)).await?;
        let records = within(
            self.timeout,
            sqlx::query(AssertSqlSafe(select)).fetch_all(&mut *snapshot),
        )
        .await?;
        within(self.timeout, snapshot.commit()).await?;
        Ok(records
            .iter()
            .map(|record| decode(record, &SILVER_COLUMNS))
            .collect::<Result<Vec<_>, _>>()?)
    }

    /// `_ship_silver_rows`.
    async fn ship_silver(&self, rows: &[Vec<Cell>]) -> Result<(), SyncError> {
        let (silver, staging) = (&self.names.silver, &self.names.silver_staging);
        self.ch.ensure_database("silver").await?;
        self.ch.execute(&ddl(SILVER_DDL, silver)).await?;
        self.ch.execute(&ddl(SILVER_DDL, staging)).await?;
        self.ch
            .execute(&format!("TRUNCATE TABLE {staging}"))
            .await?;
        if !rows.is_empty() {
            let body = tsv::render(rows, &SILVER_COLUMNS)?;
            self.ch
                .insert(
                    &format!(
                        "INSERT INTO {staging} ({}) FORMAT TabSeparated",
                        tsv::names(&SILVER_COLUMNS)
                    ),
                    body,
                )
                .await?;
        }
        let staged = number(
            &self
                .ch
                .execute(&format!("SELECT count() FROM {staging}"))
                .await?,
        )?;
        if staged != rows_i64(rows.len()) {
            return Err(SyncError::Short {
                staged,
                supplied: rows.len(),
            });
        }
        self.ch
            .execute(&format!("EXCHANGE TABLES {silver} AND {staging}"))
            .await?;
        Ok(())
    }

    /// `_derive_gold`: recompute Gold from the Silver just shipped, atomically.
    async fn derive_gold(&self) -> Result<i64, SyncError> {
        let (gold, staging) = (&self.names.gold, &self.names.gold_staging);
        self.ch.ensure_database("gold").await?;
        self.ch.execute(&ddl(GOLD_DDL, gold)).await?;
        self.ch.execute(&ddl(GOLD_DDL, staging)).await?;
        self.ch
            .execute(&format!("TRUNCATE TABLE {staging}"))
            .await?;
        let (fine, rollup) = derive_gold_sql(&self.names);
        self.ch.execute(&fine).await?;
        self.ch.execute(&rollup).await?;
        let rows = number(
            &self
                .ch
                .execute(&format!("SELECT count() FROM {staging}"))
                .await?,
        )?;
        self.ch
            .execute(&format!("EXCHANGE TABLES {gold} AND {staging}"))
            .await?;
        Ok(rows)
    }

    async fn history(&self) -> Value {
        match self.ship_history().await {
            Ok(appended) => json!({ "appended": appended }),
            Err(error) => {
                let detail = error.to_string();
                tracing::error!("ch_history: ship failed: {detail}");
                watermark::mark_failed(self.pg, self.timeout, &self.names.history_state, &detail)
                    .await;
                json!({ "error": detail })
            }
        }
    }

    /// `ship_history`: every version Postgres holds that the copy lacks.
    async fn ship_history(&self) -> Result<usize, SyncError> {
        let history = &self.names.history;
        self.ch.ensure_database(&self.names.history_db).await?;
        self.ch.execute(&ddl(HISTORY_DDL, history)).await?;
        // The ceiling BEFORE shipping is what tells a heal from an append.
        let ceiling = number(
            &self
                .ch
                .execute(&format!("SELECT coalesce(max(id), 0) FROM {history}"))
                .await?,
        )?;
        let copied = self
            .ch
            .execute(&format!("SELECT id FROM {history} FORMAT TabSeparated"))
            .await?;
        let mut held = HashSet::new();
        for line in copied.split('\n').filter(|line| !line.is_empty()) {
            held.insert(number(line)?);
        }
        let ids: Vec<i64> = within(
            self.timeout,
            sqlx::query_scalar("SELECT id FROM app.order_versions").fetch_all(self.pg),
        )
        .await?;
        let mut missing: Vec<i64> = ids.into_iter().filter(|id| !held.contains(id)).collect();
        missing.sort_unstable();
        missing.dedup();

        let select = format!(
            "SELECT {} FROM app.order_versions WHERE id = ANY($1::bigint[]) ORDER BY id",
            tsv::names(&HISTORY_COLUMNS)
        );
        let insert = format!(
            "INSERT INTO {history} ({}) FORMAT TabSeparated",
            tsv::names(&HISTORY_COLUMNS)
        );
        let mut shipped = 0;
        for chunk in missing.chunks(self.history_chunk.max(1)) {
            let records = within(
                self.timeout,
                sqlx::query(AssertSqlSafe(select.clone()))
                    .bind(chunk.to_vec())
                    .fetch_all(self.pg),
            )
            .await?;
            let rows = records
                .iter()
                .map(|record| decode(record, &HISTORY_COLUMNS))
                .collect::<Result<Vec<_>, _>>()?;
            self.ch
                .insert(&insert, tsv::render(&rows, &HISTORY_COLUMNS)?)
                .await?;
            shipped += rows.len();
        }
        watermark::mark_ok(
            self.pg,
            self.timeout,
            &self.names.history_state,
            rows_i64(shipped),
        )
        .await?;
        let healed = missing.iter().filter(|&&id| id <= ceiling).count();
        if healed > 0 {
            let message = format!(
                "the hourly ids-diff re-shipped {healed} version(s) below {history}'s \
                 watermark — rows the copy had lost"
            );
            tracing::warn!("ch_history: {message}");
            self.alerts.raise(&Finding {
                key: "ch_history_selfhealed".into(),
                severity: Severity::Info,
                count: u64::try_from(healed).unwrap_or(u64::MAX),
                message,
            });
        } else if shipped > 0 {
            tracing::info!("ch_history: appended {shipped} version(s) to {history}");
        }
        Ok(shipped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_shadow_writes_only_its_own_names() {
        let live = Names::new(Target::Live, "history");
        let shadow = Names::new(Target::Shadow, "history");
        assert_eq!(live.silver, "silver.orders");
        assert_eq!(shadow.silver, "silver.orders_rust");
        assert_eq!(shadow.silver_staging, "silver.orders_rust_staging");
        assert_eq!(shadow.gold_staging, "gold.daily_revenue_rust_staging");
        assert_eq!(shadow.history, "history.order_versions_rust");
        assert_eq!(
            shadow.gold_state,
            "rust_shadow.clickhouse.gold_daily_revenue"
        );
        for (a, b) in [
            (&live.silver, &shadow.silver),
            (&live.gold, &shadow.gold),
            (&live.history, &shadow.history),
            (&live.silver_state, &shadow.silver_state),
        ] {
            assert_ne!(a, b);
        }
    }

    #[test]
    fn the_silver_column_list_is_the_projections() {
        let names: Vec<&str> = SILVER_COLUMNS.iter().map(|c| c.name).collect();
        assert_eq!(names, warehouse::SILVER_COLUMNS);
    }

    #[test]
    fn counts_read_as_python_reads_them() {
        assert_eq!(number("46698\n").unwrap(), 46_698);
        assert_eq!(number("").unwrap(), 0);
        assert!(number("Code: 60").is_err());
    }
}
