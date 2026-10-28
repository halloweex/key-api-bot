//! `/api/margin/*`: the /margin tab's six GET routes, answered as
//! `web/routes/api/margin.py` answers them under `KS_READ_MARGIN=postgres` —
//! the statements `core/repositories/margin.py` renders for Postgres, the
//! Python arithmetic over their rows, the bytes orjson writes.
//!
//! The parity is by contract: what the frontend sends — a period or two
//! dates, the sales type, and the header's four filters, which these routes
//! ignore — must answer byte for byte. `limit`, `min_revenue` and
//! `margin_floor` the frontend never sends, so they are read plainly here
//! rather than by pydantic's lax rules; where the two disagree on hostile
//! input, the harness's list says so.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::postgres::PgRow;
use sqlx::{AssertSqlSafe, PgPool, Row};

use crate::gold::decimal_to_float;
use crate::params::FieldError;
use crate::period;
use crate::pyrt::{PyNum, round_float};
use crate::trend::{Failure, ReadError};
use crate::validate;

/// The surface `read_fallback` names in a 503.
const SURFACE: &str = "margin";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    Overview,
    ByBrand,
    ByCategory,
    Trend,
    BrandCategory,
    Alerts,
}

impl Route {
    /// The route for a contract endpoint name, if it is one of margin's.
    pub fn from_endpoint(name: &str) -> Option<Self> {
        Some(match name {
            "web.routes.api.margin.get_margin_overview" => Self::Overview,
            "web.routes.api.margin.get_margin_by_brand" => Self::ByBrand,
            "web.routes.api.margin.get_margin_by_category" => Self::ByCategory,
            "web.routes.api.margin.get_margin_trend" => Self::Trend,
            "web.routes.api.margin.get_margin_brand_category" => Self::BrandCategory,
            "web.routes.api.margin.get_margin_alerts" => Self::Alerts,
            _ => return None,
        })
    }
}

/// The route's parameters after FastAPI, before its own validators.
struct Query {
    period: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
    sales_type: String,
    limit: i64,
    min_revenue: Decimal,
    margin_floor: f64,
}

fn field_error(
    kind: &'static str,
    name: &'static str,
    msg: &'static str,
    input: &str,
) -> FieldError {
    FieldError {
        kind,
        loc: ("query", name),
        msg,
        input: input.to_owned(),
    }
}

/// `limit: int = Query(20, ge=1, le=50)`, read as Rust reads an integer.
fn limit(raw: Option<String>, errors: &mut Vec<FieldError>) -> i64 {
    let Some(raw) = raw else { return 20 };
    match raw.parse::<i64>() {
        Err(_) => errors.push(field_error(
            "int_parsing",
            "limit",
            "Input should be a valid integer, unable to parse string as an integer",
            &raw,
        )),
        Ok(n) if n < 1 => errors.push(field_error(
            "greater_than_equal",
            "limit",
            "Input should be greater than or equal to 1",
            &raw,
        )),
        Ok(n) if n > 50 => errors.push(field_error(
            "less_than_equal",
            "limit",
            "Input should be less than or equal to 50",
            &raw,
        )),
        Ok(n) => return n,
    }
    20
}

/// A `float = Query(default, ge=0, le=…)`, read as Rust reads a float, and
/// finite: NaN and the infinities are refused here rather than carried into
/// a bound or a bind.
fn float(
    raw: &str,
    name: &'static str,
    le: Option<(f64, &'static str)>,
    errors: &mut Vec<FieldError>,
) -> Option<f64> {
    let ge0 = "Input should be greater than or equal to 0";
    match raw.parse::<f64>() {
        Err(_) => errors.push(field_error(
            "float_parsing",
            name,
            "Input should be a valid number, unable to parse string as a number",
            raw,
        )),
        Ok(x) if !x.is_finite() => errors.push(field_error(
            "finite_number",
            name,
            "Input should be a finite number",
            raw,
        )),
        Ok(x) if x < 0.0 => errors.push(field_error("greater_than_equal", name, ge0, raw)),
        Ok(x) => match le {
            Some((bound, msg)) if x > bound => {
                errors.push(field_error("less_than_equal", name, msg, raw));
            }
            _ => return Some(x),
        },
    }
    None
}

/// asyncpg binds a Python number to a `numeric` parameter through its text;
/// the shortest round-tripping text of an `f64` is Python's `repr`.
fn to_decimal(x: f64) -> Option<Decimal> {
    format!("{x}")
        .parse::<Decimal>()
        .ok()
        .or_else(|| Decimal::from_scientific(&format!("{x:e}")).ok())
}

fn parse(route: Route, raw: &str) -> Result<Query, Failure> {
    let mut last: HashMap<String, String> = HashMap::new();
    for (name, value) in form_urlencoded::parse(raw.as_bytes()) {
        last.insert(name.into_owned(), value.into_owned());
    }
    let mut take = |name: &str| last.remove(name);
    let mut errors = Vec::new();
    let period = take("period");
    let start_date = take("start_date");
    let end_date = take("end_date");
    let sales_type = take("sales_type").unwrap_or_else(|| "retail".to_owned());
    let limit = match route {
        Route::ByBrand => limit(take("limit"), &mut errors),
        _ => 20,
    };
    // The defaults are Python `int`s and are not validated: 500 and 50 000
    // reach asyncpg as integers, exactly.
    let mut min_revenue = Decimal::from(if route == Route::Alerts { 50_000 } else { 500 });
    if route == Route::BrandCategory
        && let Some(raw) = take("min_revenue")
        && let Some(x) = float(&raw, "min_revenue", None, &mut errors)
    {
        // A finite f64 past `Decimal`'s 28 digits has no bind here.
        match to_decimal(x) {
            Some(d) => min_revenue = d,
            None => errors.push(field_error(
                "decimal_max_digits",
                "min_revenue",
                "Decimal input should have no more than 28 digits in total",
                &raw,
            )),
        }
    }
    let margin_floor = match (route, take("margin_floor")) {
        (Route::Alerts, Some(raw)) => float(
            &raw,
            "margin_floor",
            Some((100.0, "Input should be less than or equal to 100")),
            &mut errors,
        )
        .unwrap_or(30.0),
        _ => 30.0,
    };
    if !errors.is_empty() {
        return Err(Failure::Unprocessable(errors));
    }
    Ok(Query {
        period,
        start_date,
        end_date,
        sales_type,
        limit,
        min_revenue,
        margin_floor,
    })
}

// ─── The statements ─────────────────────────────────────────────────────────
//
// `core/repositories/margin.py`'s texts as `render_tables(sql, POSTGRES)` and
// `numbered()` leave them, so both services ask the planner one question.
// The line predicate is `core.duckdb_constants.line_window_where`.

const COSTED: &str = "os.purchased_price IS NOT NULL AND os.purchased_price > 0";

const ROOT_CAT: &str = "WITH root_cat AS (
                SELECT c1.id,
                       COALESCE(c2.name, c1.name) as root_name
                FROM bronze.categories c1
                LEFT JOIN bronze.categories c2 ON c1.parent_id = c2.id
            )";

/// The WHERE clause and the next free placeholder.
fn line_window_where(sales_type: &str) -> (String, usize) {
    let mut clauses =
        String::from("l.order_date BETWEEN $1 AND $2 AND NOT l.is_return AND l.is_active_source");
    if sales_type == "all" {
        (clauses, 3)
    } else {
        clauses.push_str(" AND l.sales_type = $3");
        (clauses, 4)
    }
}

fn statement(route: Route, sales_type: &str) -> String {
    let (wh, next) = line_window_where(sales_type);
    let lines = "FROM silver.order_lines l
            LEFT JOIN bronze.offer_stocks os ON l.sku = os.sku";
    let costed_revenue = format!("SUM(CASE WHEN {COSTED} THEN l.line_amount ELSE 0 END)");
    let cogs = format!("SUM(CASE WHEN {COSTED} THEN os.purchased_price * l.quantity ELSE 0 END)");
    let brand = "COALESCE(NULLIF(TRIM(l.brand), ''), 'Unknown') as brand";
    match route {
        Route::Overview => format!(
            "SELECT
                COALESCE(SUM(l.line_amount), 0) as total_revenue,
                COALESCE({costed_revenue}, 0) as costed_revenue,
                COALESCE({cogs}, 0) as cogs,
                COUNT(DISTINCT CASE WHEN {COSTED} THEN l.sku END) as skus_with_cost,
                COUNT(DISTINCT l.sku) as total_skus,
                COALESCE(SUM(l.quantity), 0) as total_units
            {lines}
            WHERE {wh}"
        ),
        Route::ByBrand => format!(
            "SELECT
                {brand},
                SUM(l.quantity) as total_units,
                ROUND(SUM(l.line_amount), 2) as total_revenue,
                SUM(CASE WHEN {COSTED} THEN l.quantity ELSE 0 END) as costed_units,
                ROUND({costed_revenue}, 2) as costed_revenue,
                ROUND({cogs}, 2) as cogs
            {lines}
            WHERE {wh}
              AND TRIM(l.brand) != '' AND l.brand IS NOT NULL
            GROUP BY 1
            ORDER BY total_revenue DESC, 1
            LIMIT ${next}"
        ),
        Route::ByCategory => format!(
            "{ROOT_CAT}
            SELECT
                COALESCE(rc.root_name, 'Uncategorized') as category,
                SUM(l.quantity) as total_units,
                ROUND(SUM(l.line_amount), 2) as total_revenue,
                ROUND({costed_revenue}, 2) as costed_revenue,
                ROUND({cogs}, 2) as cogs,
                ROUND(100.0 * SUM(l.line_amount)
                    / SUM(SUM(l.line_amount)) OVER (), 1) as rev_share_pct
            {lines}
            LEFT JOIN root_cat rc ON l.category_id = rc.id
            WHERE {wh}
            GROUP BY 1
            ORDER BY total_revenue DESC, 1"
        ),
        Route::Trend => format!(
            "SELECT
                substr(CAST(l.order_date AS VARCHAR), 1, 7) as month,
                ROUND({costed_revenue}, 2) as costed_revenue,
                ROUND({cogs}, 2) as cogs,
                ROUND(SUM(l.line_amount), 2) as total_revenue
            {lines}
            WHERE {wh}
            GROUP BY 1
            ORDER BY 1"
        ),
        Route::BrandCategory => format!(
            "{ROOT_CAT}
            SELECT
                {brand},
                COALESCE(rc.root_name, 'Uncategorized') as category,
                SUM(l.quantity) as total_units,
                ROUND(SUM(l.line_amount), 2) as total_revenue,
                ROUND({costed_revenue}, 2) as costed_revenue,
                ROUND({cogs}, 2) as cogs
            {lines}
            LEFT JOIN root_cat rc ON l.category_id = rc.id
            WHERE {wh}
            GROUP BY 1, 2
            HAVING SUM(l.line_amount) > ${next}
            ORDER BY brand, total_revenue DESC, 2"
        ),
        Route::Alerts => format!(
            "SELECT
                {brand},
                SUM(l.quantity) as total_units,
                ROUND(SUM(l.line_amount), 2) as total_revenue,
                ROUND({costed_revenue}, 2) as costed_revenue,
                ROUND({cogs}, 2) as cogs
            {lines}
            WHERE {wh}
            GROUP BY 1
            HAVING {costed_revenue} > ${next}
            ORDER BY total_revenue DESC, 1"
        ),
    }
}

// ─── The store ──────────────────────────────────────────────────────────────

pub struct PgMargin {
    pool: PgPool,
    /// asyncpg's `command_timeout` (`KS_PG_TIMEOUT`, 30 s).
    timeout: Duration,
    /// `REQUIRED_REVISION`: `pg_margin_read.fetch` checks it before every read.
    revision: String,
}

impl PgMargin {
    pub fn new(pool: PgPool, timeout: Duration, revision: String) -> Self {
        Self {
            pool,
            timeout,
            revision,
        }
    }

    async fn rows(
        &self,
        route: Route,
        q: &Query,
        dates: (NaiveDate, NaiveDate),
    ) -> Result<Vec<PgRow>, ReadError> {
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
            let mut query = sqlx::query(AssertSqlSafe(statement(route, &q.sales_type)))
                .bind(dates.0)
                .bind(dates.1);
            if q.sales_type != "all" {
                query = query.bind(q.sales_type.clone());
            }
            query = match route {
                Route::ByBrand => query.bind(q.limit),
                Route::BrandCategory | Route::Alerts => query.bind(q.min_revenue),
                _ => query,
            };
            query.fetch_all(&self.pool).await
        };
        match tokio::time::timeout(self.timeout, work).await {
            Ok(Ok(rows)) => Ok(rows),
            Ok(Err(error)) => Err(ReadError(error.to_string())),
            Err(_) => Err(ReadError(format!("no answer within {:?}", self.timeout))),
        }
    }
}

// ─── The answers ────────────────────────────────────────────────────────────

/// `float(r[i])`: a `None` there is Python's TypeError, the app's 500.
fn num(row: &PgRow, i: usize) -> Result<f64, Failure> {
    let value: Option<Decimal> = row
        .try_get(i)
        .map_err(|_| Failure::Raised("a column is not numeric"))?;
    value
        .map(decimal_to_float)
        .ok_or(Failure::Raised("float(None)"))
}

fn int(row: &PgRow, i: usize) -> Result<Option<i64>, Failure> {
    row.try_get(i)
        .map_err(|_| Failure::Raised("a column is not bigint"))
}

fn text(row: &PgRow, i: usize) -> Result<Option<String>, Failure> {
    row.try_get(i)
        .map_err(|_| Failure::Raised("a column is not text"))
}

/// `round(100.0 * a / b, 1) if b > 0`, else nothing.
fn pct(a: f64, b: f64) -> Option<f64> {
    (b > 0.0).then(|| round_float(100.0 * a / b, 1))
}

/// The same, with the `int` 0 for the else.
fn pct_or_zero(a: f64, b: f64) -> PyNum {
    pct(a, b).map_or(PyNum::Int(0), PyNum::Float)
}

#[derive(Serialize)]
struct Overview {
    total_revenue: f64,
    costed_revenue: f64,
    cogs: f64,
    profit: f64,
    margin_pct: PyNum,
    coverage_pct: PyNum,
    skus_with_cost: Option<i64>,
    total_skus: Option<i64>,
    total_units: Option<i64>,
}

#[derive(Serialize)]
struct ByBrand {
    brand: Option<String>,
    total_units: Option<i64>,
    total_revenue: f64,
    costed_units: Option<i64>,
    costed_revenue: f64,
    cogs: f64,
    profit: f64,
    margin_pct: Option<f64>,
    coverage_pct: PyNum,
}

#[derive(Serialize)]
struct ByCategory {
    category: Option<String>,
    total_units: Option<i64>,
    total_revenue: f64,
    costed_revenue: f64,
    cogs: f64,
    profit: f64,
    margin_pct: Option<f64>,
    coverage_pct: PyNum,
    rev_share_pct: PyNum,
}

#[derive(Serialize)]
struct Month {
    month: Option<String>,
    revenue: f64,
    cogs: f64,
    profit: f64,
    margin_pct: PyNum,
    total_revenue: f64,
    coverage_pct: PyNum,
}

#[derive(Serialize)]
struct BrandCategory {
    brand: Option<String>,
    category: Option<String>,
    total_units: Option<i64>,
    total_revenue: f64,
    costed_revenue: f64,
    cogs: f64,
    profit: f64,
    margin_pct: Option<f64>,
    coverage_pct: PyNum,
}

#[derive(Serialize)]
struct Alert {
    brand: Option<String>,
    total_units: Option<i64>,
    total_revenue: f64,
    costed_revenue: f64,
    cogs: f64,
    profit: f64,
    margin_pct: PyNum,
    margin_floor: f64,
    impact: f64,
}

fn body(route: Route, rows: &[PgRow], margin_floor: f64) -> Result<Vec<u8>, Failure> {
    let bytes = match route {
        Route::Overview => {
            let row = rows
                .first()
                .ok_or(Failure::Raised("an aggregate returned no row"))?;
            let (total, costed, cogs) = (num(row, 0)?, num(row, 1)?, num(row, 2)?);
            let profit = costed - cogs;
            serde_json::to_vec(&Overview {
                total_revenue: round_float(total, 2),
                costed_revenue: round_float(costed, 2),
                cogs: round_float(cogs, 2),
                profit: round_float(profit, 2),
                margin_pct: pct_or_zero(profit, costed),
                coverage_pct: pct_or_zero(costed, total),
                skus_with_cost: int(row, 3)?,
                total_skus: int(row, 4)?,
                total_units: int(row, 5)?,
            })
        }
        Route::ByBrand => {
            let mut out = Vec::with_capacity(rows.len());
            for r in rows {
                let (costed, cogs, total) = (num(r, 4)?, num(r, 5)?, num(r, 2)?);
                let profit = costed - cogs;
                out.push(ByBrand {
                    brand: text(r, 0)?,
                    total_units: int(r, 1)?,
                    total_revenue: total,
                    costed_units: int(r, 3)?,
                    costed_revenue: costed,
                    cogs,
                    profit: round_float(profit, 2),
                    margin_pct: pct(profit, costed),
                    coverage_pct: pct_or_zero(costed, total),
                });
            }
            serde_json::to_vec(&out)
        }
        Route::ByCategory => {
            let mut out = Vec::with_capacity(rows.len());
            for r in rows {
                let (costed, cogs, total) = (num(r, 3)?, num(r, 4)?, num(r, 2)?);
                let profit = costed - cogs;
                // `float(r[5]) if r[5] else 0`: a NULL or a zero is the int 0.
                let share: Option<Decimal> = r
                    .try_get(5)
                    .map_err(|_| Failure::Raised("a column is not numeric"))?;
                out.push(ByCategory {
                    category: text(r, 0)?,
                    total_units: int(r, 1)?,
                    total_revenue: total,
                    costed_revenue: costed,
                    cogs,
                    profit: round_float(profit, 2),
                    margin_pct: pct(profit, costed),
                    coverage_pct: pct_or_zero(costed, total),
                    rev_share_pct: share
                        .filter(|d| !d.is_zero())
                        .map_or(PyNum::Int(0), |d| PyNum::Float(decimal_to_float(d))),
                });
            }
            serde_json::to_vec(&out)
        }
        Route::Trend => {
            let mut out = Vec::with_capacity(rows.len());
            for r in rows {
                let (costed, cogs, total) = (num(r, 1)?, num(r, 2)?, num(r, 3)?);
                let profit = costed - cogs;
                out.push(Month {
                    month: text(r, 0)?,
                    revenue: costed,
                    cogs,
                    profit: round_float(profit, 2),
                    margin_pct: pct_or_zero(profit, costed),
                    total_revenue: total,
                    coverage_pct: pct_or_zero(costed, total),
                });
            }
            serde_json::to_vec(&out)
        }
        Route::BrandCategory => {
            let mut out = Vec::with_capacity(rows.len());
            for r in rows {
                let (costed, cogs, total) = (num(r, 4)?, num(r, 5)?, num(r, 3)?);
                let profit = costed - cogs;
                out.push(BrandCategory {
                    brand: text(r, 0)?,
                    category: text(r, 1)?,
                    total_units: int(r, 2)?,
                    total_revenue: total,
                    costed_revenue: costed,
                    cogs,
                    profit: round_float(profit, 2),
                    margin_pct: pct(profit, costed),
                    coverage_pct: pct_or_zero(costed, total),
                });
            }
            serde_json::to_vec(&out)
        }
        Route::Alerts => {
            let mut out = Vec::new();
            for r in rows {
                let (costed, cogs) = (num(r, 3)?, num(r, 4)?);
                let profit = costed - cogs;
                let margin = if costed > 0.0 {
                    PyNum::Float(100.0 * profit / costed)
                } else {
                    PyNum::Int(0)
                };
                if margin.as_f64() < margin_floor {
                    let target_profit = costed * margin_floor / 100.0;
                    out.push(Alert {
                        brand: text(r, 0)?,
                        total_units: int(r, 1)?,
                        total_revenue: num(r, 2)?,
                        costed_revenue: costed,
                        cogs,
                        profit: round_float(profit, 2),
                        margin_pct: match margin {
                            PyNum::Float(m) => PyNum::Float(round_float(m, 1)),
                            zero => zero,
                        },
                        margin_floor,
                        impact: round_float(target_profit - profit, 2),
                    });
                }
            }
            // `alerts.sort(key=impact, reverse=True)`: stable, so equal
            // impacts keep the statement's order.
            out.sort_by(|a, b| {
                b.impact
                    .partial_cmp(&a.impact)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            serde_json::to_vec(&out)
        }
    };
    Ok(bytes.expect("the body is plain data"))
}

/// The JSON body for a 200, or the reason there is none. `admit` is the
/// route's rate limit, which slowapi checks once the parameters parse and
/// before the route's own validators run.
pub async fn answer(
    route: Route,
    store: &PgMargin,
    raw_query: &str,
    now: DateTime<Utc>,
    admit: impl FnOnce() -> Result<(), Failure>,
) -> Result<Vec<u8>, Failure> {
    let q = parse(route, raw_query)?;
    admit()?;
    let bad = Failure::BadRequest;
    validate::period(q.period.as_deref()).map_err(bad)?;
    let sales_type = validate::sales_type(&q.sales_type).map_err(bad)?;
    let q = Query { sales_type, ..q };
    let dates = period::parse_period(
        q.period.as_deref(),
        q.start_date.as_deref(),
        q.end_date.as_deref(),
        period::kyiv_today(now),
    )?;
    let rows = store
        .rows(route, &q, dates)
        .await
        .map_err(|e| Failure::Unavailable(SURFACE, e))?;
    body(route, &rows, q.margin_floor)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn errors(route: Route, query: &str) -> Vec<&'static str> {
        match parse(route, query) {
            Err(Failure::Unprocessable(errors)) => errors.iter().map(|e| e.kind).collect(),
            Err(other) => panic!("{other:?}"),
            Ok(_) => vec![],
        }
    }

    #[test]
    fn the_routes_own_parameters_are_read_only_where_they_exist() {
        // `limit` belongs to by-brand alone: elsewhere FastAPI ignores it.
        assert_eq!(errors(Route::ByBrand, "limit=0"), ["greater_than_equal"]);
        assert_eq!(errors(Route::ByBrand, "limit=51"), ["less_than_equal"]);
        assert_eq!(errors(Route::ByBrand, "limit=x"), ["int_parsing"]);
        assert!(errors(Route::Overview, "limit=x&min_revenue=y&margin_floor=z").is_empty());
        assert_eq!(
            errors(Route::BrandCategory, "min_revenue=-1"),
            ["greater_than_equal"]
        );
        assert_eq!(
            errors(Route::Alerts, "margin_floor=101"),
            ["less_than_equal"]
        );
        for spelling in ["nan", "inf", "-inf"] {
            let query = format!("margin_floor={spelling}&min_revenue={spelling}");
            assert_eq!(errors(Route::Alerts, &query), ["finite_number"]);
            assert_eq!(errors(Route::BrandCategory, &query), ["finite_number"]);
        }
        assert_eq!(
            errors(Route::BrandCategory, "min_revenue=1e300"),
            ["decimal_max_digits"]
        );
    }

    #[test]
    fn the_header_filters_are_ignored_as_fastapi_ignores_them() {
        for route in [Route::Overview, Route::Trend, Route::Alerts] {
            assert!(errors(route, "source_id=abc&category_id=-1&brand=&promocode=x").is_empty());
        }
    }

    #[test]
    fn defaults_reach_postgres_as_python_left_them() {
        assert_eq!(
            parse(Route::BrandCategory, "").unwrap().min_revenue,
            Decimal::from(500)
        );
        assert_eq!(
            parse(Route::Alerts, "").unwrap().min_revenue,
            Decimal::from(50_000)
        );
        assert_eq!(parse(Route::ByBrand, "").unwrap().limit, 20);
        assert_eq!(parse(Route::Alerts, "").unwrap().margin_floor, 30.0);
        assert_eq!(to_decimal(0.1), Some("0.1".parse().unwrap()));
    }

    #[test]
    fn the_sales_type_clause_shifts_the_placeholders() {
        assert_eq!(line_window_where("all").1, 3);
        assert_eq!(line_window_where("retail").1, 4);
        assert!(statement(Route::ByBrand, "retail").contains("LIMIT $4"));
        assert!(statement(Route::Alerts, "all").contains("> $3"));
    }
}
