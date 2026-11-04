//! The Postgres half: `core/pg_gold_read.fetch_series`, statement for
//! statement. The SQL is the Python's own text, so both services ask the
//! planner the same question and the benchmark measures the services.

use std::time::Duration;

use chrono::NaiveDate;
use rust_decimal::Decimal;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};

use crate::pgread;
use crate::trend::{DayRow, Gold, ReadError};

// The four texts `fetch_series` renders. Static, which sqlx 0.9 asks of any
// statement it is not told is safe — and which is also what makes each one
// prepared once per connection, as asyncpg's statement cache does in Python.
const ONE_TYPE_ALL_SOURCES: &str = "SELECT date, revenue, orders_count \
     FROM gold.daily_revenue \
     WHERE date BETWEEN $1 AND $2 AND source_id IS NULL AND sales_type = $3 \
     ORDER BY date";
const ONE_TYPE_ONE_SOURCE: &str = "SELECT date, revenue, orders_count \
     FROM gold.daily_revenue \
     WHERE date BETWEEN $1 AND $2 AND source_id = $3 AND sales_type = $4 \
     ORDER BY date";
const ALL_TYPES_ALL_SOURCES: &str = "SELECT date, SUM(revenue), SUM(orders_count) \
     FROM gold.daily_revenue \
     WHERE date BETWEEN $1 AND $2 AND source_id IS NULL \
     GROUP BY date ORDER BY date";
const ALL_TYPES_ONE_SOURCE: &str = "SELECT date, SUM(revenue), SUM(orders_count) \
     FROM gold.daily_revenue \
     WHERE date BETWEEN $1 AND $2 AND source_id = $3 \
     GROUP BY date ORDER BY date";

pub struct PgGold {
    pool: PgPool,
    /// asyncpg's `command_timeout` (`KS_PG_TIMEOUT`, 30 s).
    timeout: Duration,
    /// `REQUIRED_REVISION`. `fetch_series` does not ask it; the pilot does,
    /// so that in a migration window every route it answers hands over.
    revision: String,
}

impl PgGold {
    pub fn new(pool: PgPool, timeout: Duration, revision: String) -> Self {
        Self {
            pool,
            timeout,
            revision,
        }
    }
}

/// `float(Decimal)`. CPython computes it as `float(str(d))` — the double
/// nearest the exact decimal — and so does this, through the same text.
/// `rust_decimal`'s own `to_f64` scales and rounds in binary instead: it
/// happens to agree for two places below 2^53 cents and promises nothing.
pub fn decimal_to_float(d: Decimal) -> f64 {
    d.to_string()
        .parse()
        .expect("a decimal's text is a float literal")
}

fn decode(row: &PgRow, summed: bool) -> Result<DayRow, sqlx::Error> {
    // `SUM(integer)` is `bigint` in Postgres and the per-type rows are
    // `integer`. asyncpg hands Python an `int` either way; sqlx refuses the
    // wrong width, so the statement decides how the column is read.
    let orders = if summed {
        row.try_get::<i64, _>(2)?
    } else {
        i64::from(row.try_get::<i32, _>(2)?)
    };
    Ok(DayRow {
        date: row.try_get(0)?,
        revenue: decimal_to_float(row.try_get(1)?),
        orders,
    })
}

impl Gold for PgGold {
    async fn series(
        &self,
        start: NaiveDate,
        end: NaiveDate,
        sales_type: &str,
        source_id: Option<i32>,
    ) -> Result<Vec<DayRow>, ReadError> {
        let summed = sales_type == "all";
        let query = match (summed, source_id) {
            (false, None) => sqlx::query(ONE_TYPE_ALL_SOURCES)
                .bind(start)
                .bind(end)
                .bind(sales_type),
            (false, Some(source)) => sqlx::query(ONE_TYPE_ONE_SOURCE)
                .bind(start)
                .bind(end)
                .bind(source)
                .bind(sales_type),
            (true, None) => sqlx::query(ALL_TYPES_ALL_SOURCES).bind(start).bind(end),
            (true, Some(source)) => sqlx::query(ALL_TYPES_ONE_SOURCE)
                .bind(start)
                .bind(end)
                .bind(source),
        };
        let mut conn = pgread::connection(&self.pool, &self.revision, self.timeout).await?;
        let rows = pgread::fetch(query.fetch_all(&mut *conn), self.timeout).await?;
        rows.iter()
            .map(|row| decode(row, summed))
            .collect::<Result<_, _>>()
            .map_err(|e| ReadError::Failed(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_numeric_becomes_the_nearest_double() {
        let d = |s: &str| decimal_to_float(s.parse().unwrap());
        assert_eq!(d("1234.50"), 1234.5);
        assert_eq!(d("0.10"), 0.1);
        assert_eq!(d("999999999999.99"), 999_999_999_999.99);
        assert_eq!(d("0.00"), 0.0);
    }
}
