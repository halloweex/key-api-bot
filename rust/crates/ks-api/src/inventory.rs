//! The inventory routes' ten GETs — `/api/stocks/summary`, `/average`,
//! `/trend`, `/analysis`, `/turnover`, `/skus`, `/brand-rotation`,
//! `/abc/{abc_class}`, `/actions` and `/alerts` — answered as
//! `web/routes/api/inventory.py` answers them under `KS_READ_INVENTORY=postgres`:
//! the statements `core/repositories/inventory.py` renders for Postgres, over
//! the eleven views revision 0014 put in `gold` and three tables, then the
//! Python over the rows, float by float. Every "today" is the database's
//! (`TODAY_IN_KYIV`); nothing here reads a clock of its own.
//!
//! A numeric reaches Python as a `Decimal` and leaves the route three ways,
//! and each is read here as it leaves: `float(x)` as `x::float8` (both round
//! the exact text once), `int(x)` cut from the exact text, and a value handed
//! on untouched as FastAPI's encoder writes it — an `int` at scale 0, a
//! float otherwise. A cast column is aliased apart from any name an ORDER BY
//! sorts by, so the order stays the exact numeric one.
//!
//! The NPV decision, the quadrant matrix, the optimal-stock model and the
//! brand scorecard are the Python arithmetic as it stands: `max(0, x)`'s int
//! zero, `sum()` compensated, `sorted(reverse=True)` stable, `round()` to
//! even, a sum of nothing the `int` 0.

use std::cmp::Ordering;
use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::{Decimal, RoundingStrategy};
use serde::Serialize;
use serde::ser::{SerializeMap, Serializer};
use sqlx::Row;
use sqlx::postgres::PgRow;

use crate::gold::decimal_to_float;
use crate::params::{FieldError, PyInt, int_message, lax_int};
use crate::pgread::{Bind, PgReads};
use crate::pyrt::{PyNum, py_sum, round_float, round_num};
use crate::trend::Failure;

/// The surface `read_fallback` names in a 503.
const SURFACE: &str = "inventory";
/// `TODAY_IN_KYIV`.
const TODAY: &str = "(now() AT TIME ZONE 'Europe/Kyiv')::date";
const SKU_STATUS: &str = "app.sku_inventory_status";
const HISTORY: &str = "app.inventory_history";
const OFFER_STOCKS: &str = "bronze.offer_stocks";
/// `get_dead_stock_deep`'s baseline for the excess capital cost.
const OPTIMAL_DAYS: i64 = 60;
const TIERS: [&str; 5] = ["hot", "healthy", "warm", "cold", "frozen"];
const ABCS: [&str; 3] = ["A", "B", "C"];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    Summary,
    Average,
    Trend,
    Analysis,
    Turnover,
    Skus,
    BrandRotation,
    Abc,
    Actions,
    Alerts,
}

impl Route {
    /// The route for a contract endpoint name, if it is one of these.
    pub fn from_endpoint(name: &str) -> Option<Self> {
        Some(match name {
            "web.routes.api.inventory.get_stock_summary" => Self::Summary,
            "web.routes.api.inventory.get_average_inventory" => Self::Average,
            "web.routes.api.inventory.get_inventory_trend" => Self::Trend,
            "web.routes.api.inventory.get_inventory_analysis" => Self::Analysis,
            "web.routes.api.inventory.get_inventory_turnover" => Self::Turnover,
            "web.routes.api.inventory.get_all_skus" => Self::Skus,
            "web.routes.api.inventory.get_brand_rotation" => Self::BrandRotation,
            "web.routes.api.inventory.get_abc_skus" => Self::Abc,
            "web.routes.api.inventory.get_stock_actions" => Self::Actions,
            "web.routes.api.inventory.get_stock_alerts" => Self::Alerts,
            _ => return None,
        })
    }
}

/// A `dict` as orjson writes it: in insertion order.
struct Ordered<V>(Vec<(&'static str, V)>);

impl<V: Serialize> Serialize for Ordered<V> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(Some(self.0.len()))?;
        for (k, v) in &self.0 {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

// ─── Python's numbers ───────────────────────────────────────────────────────

/// `float(x or 0)`: a NULL and a zero are `0.0`.
fn or_zero(v: Option<f64>) -> f64 {
    match v {
        Some(x) if x != 0.0 => x,
        _ => 0.0,
    }
}

/// `max(0, x)`: the `int` 0 unless `x` is greater.
fn max0(x: f64) -> PyNum {
    if x > 0.0 {
        PyNum::Float(x)
    } else {
        PyNum::Int(0)
    }
}

/// `a > b` between two of Python's numbers.
fn gt(a: PyNum, b: f64) -> bool {
    a.as_f64() > b
}

/// `a - b`: an `int` only when both are.
fn minus(a: PyNum, b: PyNum) -> PyNum {
    match (a, b) {
        (PyNum::Int(x), PyNum::Int(y)) => PyNum::Int(x - y),
        _ => PyNum::Float(a.as_f64() - b.as_f64()),
    }
}

/// `round(x)` with no digits: the nearest integer, ties to even, an `int`.
fn round_int(x: f64) -> Result<i64, Failure> {
    if !x.is_finite() {
        return Err(Failure::Raised("round() of a float that is not finite"));
    }
    Ok(x.round_ties_even() as i64)
}

/// `int(x)` of a float: toward zero.
fn trunc_int(x: f64) -> Result<i64, Failure> {
    if !x.is_finite() {
        return Err(Failure::Raised("int() of a float that is not finite"));
    }
    Ok(x.trunc() as i64)
}

/// `int(Decimal)`: the exact text cut at its point.
fn trunc_numeric(text: &str) -> Result<i64, Failure> {
    let head = text.split_once('.').map_or(text, |(h, _)| h);
    head.parse()
        .map_err(|_| Failure::Raised("int() of a numeric that is not a 64-bit integer"))
}

/// `round(Decimal)`: to an `int`, ties to even, on the exact text.
fn round_numeric(text: &str) -> Result<i64, Failure> {
    let (negative, body) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (whole, fraction) = body.split_once('.').unwrap_or((body, ""));
    let mut n: i64 = whole
        .parse()
        .map_err(|_| Failure::Raised("round() of a numeric that is not a 64-bit integer"))?;
    let first = fraction.bytes().next().unwrap_or(b'0');
    let beyond = fraction.bytes().skip(1).any(|b| b != b'0');
    if first > b'5' || (first == b'5' && (beyond || n % 2 == 1)) {
        n += 1;
    }
    Ok(if negative { -n } else { n })
}

/// A numeric handed on as `x or 0`: the `int` 0 for a NULL or a zero, else
/// FastAPI's encoder — an `int` with no fraction, a float with one.
fn numeric_or_zero(text: Option<String>) -> Result<PyNum, Failure> {
    let Some(text) = text else {
        return Ok(PyNum::Int(0));
    };
    let value: f64 = text
        .parse()
        .map_err(|_| Failure::Raised("a numeric that is not a number"))?;
    if value == 0.0 {
        return Ok(PyNum::Int(0));
    }
    if text.contains('.') {
        return Ok(PyNum::Float(value));
    }
    text.parse()
        .map(PyNum::Int)
        .map_err(|_| Failure::Raised("an integer past 64 bits"))
}

/// `min(values)`: the first of the smallest.
fn py_min(values: &[f64]) -> f64 {
    values
        .iter()
        .copied()
        .reduce(|best, x| if x < best { x } else { best })
        .unwrap_or(0.0)
}

/// `max(values)`: the first of the largest.
fn py_max(values: &[f64]) -> f64 {
    values
        .iter()
        .copied()
        .reduce(|best, x| if x > best { x } else { best })
        .unwrap_or(0.0)
}

fn floats(values: impl IntoIterator<Item = f64>) -> Vec<PyNum> {
    values.into_iter().map(PyNum::Float).collect()
}

// ─── The query string ───────────────────────────────────────────────────────

/// The route's parameters after FastAPI, before its own checks.
struct Query {
    limit: i64,
    days: i64,
    granularity: String,
    carrying_rate: f64,
    liquidation_discount: f64,
    lead_time: i64,
    safety_multiplier: f64,
    buffer_days: i64,
    max_acceptable_days: i64,
    min_skus: i64,
}

fn field_error(
    kind: &'static str,
    name: &'static str,
    msg: &'static str,
    input: Option<String>,
) -> FieldError {
    FieldError {
        kind,
        loc: ("query", name),
        msg,
        input,
    }
}

/// One bound of a `Query(…, ge=…, le=…)` and pydantic's message for it.
type Bound<T> = (T, &'static str);

/// An `int = Query(…, ge=lo, le=hi)`, read as pydantic reads it.
fn int_param(
    raw: Option<String>,
    name: &'static str,
    default: i64,
    (lo, hi): (Bound<i64>, Bound<i64>),
    errors: &mut Vec<FieldError>,
) -> i64 {
    let Some(raw) = raw else {
        return default;
    };
    let value = match lax_int(&raw) {
        Ok(PyInt::Small(v)) => v,
        Ok(PyInt::Big(_)) => {
            errors.push(field_error(
                "int_parsing",
                name,
                int_message("int_parsing"),
                Some(raw),
            ));
            return default;
        }
        Err(kind) => {
            errors.push(field_error(kind, name, int_message(kind), Some(raw)));
            return default;
        }
    };
    if value < lo.0 {
        errors.push(field_error("greater_than_equal", name, lo.1, Some(raw)));
    } else if value > hi.0 {
        errors.push(field_error("less_than_equal", name, hi.1, Some(raw)));
    }
    value
}

/// pydantic's lax `float` from a string (`str_as_float`): Rust's own float
/// grammar after `str::trim`, then once more with underscores between
/// characters taken out of the untrimmed text.
fn lax_float(raw: &str) -> Option<f64> {
    if let Ok(v) = raw.trim().parse() {
        return Some(v);
    }
    if raw.starts_with('_') || raw.ends_with('_') || raw.contains("__") {
        return None;
    }
    raw.replace('_', "").parse().ok()
}

/// A `float = Query(…, ge=lo, le=hi)`. pydantic allows inf and nan and then
/// holds them to the bounds, which neither passes.
fn float_param(
    raw: Option<String>,
    name: &'static str,
    default: f64,
    (lo, hi): (Bound<f64>, Bound<f64>),
    errors: &mut Vec<FieldError>,
) -> f64 {
    let Some(raw) = raw else {
        return default;
    };
    let Some(value) = lax_float(&raw) else {
        errors.push(field_error(
            "float_parsing",
            name,
            "Input should be a valid number, unable to parse string as a number",
            Some(raw),
        ));
        return default;
    };
    if value.is_nan() || value < lo.0 {
        errors.push(field_error("greater_than_equal", name, lo.1, Some(raw)));
    } else if value > hi.0 {
        errors.push(field_error("less_than_equal", name, hi.1, Some(raw)));
    }
    value
}

const fn ge(n: i64, msg: &'static str) -> Bound<i64> {
    (n, msg)
}

const SUMMARY_LIMIT: (Bound<i64>, Bound<i64>) = (
    ge(5, "Input should be greater than or equal to 5"),
    ge(50, "Input should be less than or equal to 50"),
);
const HISTORY_DAYS: (Bound<i64>, Bound<i64>) = (
    ge(7, "Input should be greater than or equal to 7"),
    ge(365, "Input should be less than or equal to 365"),
);
const TURNOVER_DAYS: (Bound<i64>, Bound<i64>) = (
    ge(7, "Input should be greater than or equal to 7"),
    ge(90, "Input should be less than or equal to 90"),
);
const LEAD_TIME: (Bound<i64>, Bound<i64>) = (
    ge(1, "Input should be greater than or equal to 1"),
    ge(90, "Input should be less than or equal to 90"),
);
const BUFFER_DAYS: (Bound<i64>, Bound<i64>) = (
    ge(0, "Input should be greater than or equal to 0"),
    ge(30, "Input should be less than or equal to 30"),
);
const MAX_DAYS: (Bound<i64>, Bound<i64>) = (
    ge(30, "Input should be greater than or equal to 30"),
    ge(365, "Input should be less than or equal to 365"),
);
const MIN_SKUS: (Bound<i64>, Bound<i64>) = (
    ge(1, "Input should be greater than or equal to 1"),
    ge(20, "Input should be less than or equal to 20"),
);
const ABC_LIMIT: (Bound<i64>, Bound<i64>) = (
    ge(1, "Input should be greater than or equal to 1"),
    ge(200, "Input should be less than or equal to 200"),
);
const CARRYING_RATE: (Bound<f64>, Bound<f64>) = (
    (0.05, "Input should be greater than or equal to 0.05"),
    (0.50, "Input should be less than or equal to 0.5"),
);
const LIQUIDATION_DISCOUNT: (Bound<f64>, Bound<f64>) = (
    (0.20, "Input should be greater than or equal to 0.2"),
    (0.80, "Input should be less than or equal to 0.8"),
);
const SAFETY_MULTIPLIER: (Bound<f64>, Bound<f64>) = (
    (1.0, "Input should be greater than or equal to 1.0"),
    (3.0, "Input should be less than or equal to 3.0"),
);

fn parse(route: Route, raw: &str) -> Result<Query, Failure> {
    let mut last = HashMap::new();
    for (name, value) in form_urlencoded::parse(raw.as_bytes()) {
        last.insert(name.into_owned(), value.into_owned());
    }
    let mut take = |name: &str| last.remove(name);
    let mut errors = Vec::new();
    let mut q = Query {
        limit: 0,
        days: 0,
        granularity: String::new(),
        carrying_rate: 0.25,
        liquidation_discount: 0.50,
        lead_time: 14,
        safety_multiplier: 1.5,
        buffer_days: 5,
        max_acceptable_days: 60,
        min_skus: 1,
    };
    // FastAPI reads each signature in its order.
    match route {
        Route::Summary => {
            q.limit = int_param(take("limit"), "limit", 20, SUMMARY_LIMIT, &mut errors);
        }
        Route::Average => {
            q.days = int_param(take("days"), "days", 30, HISTORY_DAYS, &mut errors);
        }
        Route::Trend => {
            q.days = int_param(take("days"), "days", 90, HISTORY_DAYS, &mut errors);
            q.granularity = take("granularity").unwrap_or_else(|| "daily".to_owned());
        }
        Route::Analysis | Route::Skus => {
            q.carrying_rate = float_param(
                take("carrying_rate"),
                "carrying_rate",
                0.25,
                CARRYING_RATE,
                &mut errors,
            );
            q.liquidation_discount = float_param(
                take("liquidation_discount"),
                "liquidation_discount",
                0.50,
                LIQUIDATION_DISCOUNT,
                &mut errors,
            );
        }
        Route::Turnover => {
            q.days = int_param(take("days"), "days", 30, TURNOVER_DAYS, &mut errors);
            q.lead_time = int_param(take("lead_time"), "lead_time", 14, LEAD_TIME, &mut errors);
            q.safety_multiplier = float_param(
                take("safety_multiplier"),
                "safety_multiplier",
                1.5,
                SAFETY_MULTIPLIER,
                &mut errors,
            );
            q.buffer_days = int_param(
                take("buffer_days"),
                "buffer_days",
                5,
                BUFFER_DAYS,
                &mut errors,
            );
            q.max_acceptable_days = int_param(
                take("max_acceptable_days"),
                "max_acceptable_days",
                60,
                MAX_DAYS,
                &mut errors,
            );
        }
        Route::BrandRotation => {
            q.min_skus = int_param(take("min_skus"), "min_skus", 1, MIN_SKUS, &mut errors);
        }
        Route::Abc => {
            q.limit = int_param(take("limit"), "limit", 50, ABC_LIMIT, &mut errors);
        }
        Route::Actions | Route::Alerts => {}
    }
    if !errors.is_empty() {
        return Err(Failure::Unprocessable(errors));
    }
    Ok(q)
}

// ─── Reading a row ──────────────────────────────────────────────────────────

fn int(row: &PgRow, i: usize) -> Result<Option<i64>, Failure> {
    if let Ok(v) = row.try_get::<Option<i64>, _>(i) {
        return Ok(v);
    }
    row.try_get::<Option<i32>, _>(i)
        .map(|v| v.map(i64::from))
        .map_err(|_| Failure::Raised("a column the statement counts is not an integer"))
}

fn f8(row: &PgRow, i: usize) -> Result<Option<f64>, Failure> {
    row.try_get(i)
        .map_err(|_| Failure::Raised("a column the statement reads as a float is not one"))
}

fn text(row: &PgRow, i: usize) -> Result<Option<String>, Failure> {
    row.try_get(i)
        .map_err(|_| Failure::Raised("a column the statement names is not text"))
}

fn decimal(row: &PgRow, i: usize) -> Result<Option<Decimal>, Failure> {
    row.try_get(i)
        .map_err(|_| Failure::Raised("a column the statement reads as a numeric is not one"))
}

fn first(rows: &[PgRow]) -> Result<&PgRow, Failure> {
    rows.first()
        .ok_or(Failure::Raised("IndexError: an aggregate returned no row"))
}

/// `datetime.isoformat()` of an aware UTC datetime: microseconds only when
/// there are any, and the offset spelled out.
fn isoformat(t: DateTime<Utc>) -> String {
    let base = t.format("%Y-%m-%dT%H:%M:%S");
    match t.timestamp_subsec_micros() {
        0 => format!("{base}+00:00"),
        micros => format!("{base}.{micros:06}+00:00"),
    }
}

/// A read through `_inventory_batch`: a failure falls back, and under
/// `KS_READ_FALLBACK=off` that is a refusal naming `inventory`.
async fn read(store: &PgReads, sql: String, binds: &[Bind]) -> Result<Vec<PgRow>, Failure> {
    store
        .rows(sql, binds)
        .await
        .map_err(|e| Failure::Unavailable(SURFACE, e))
}

fn json<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).expect("the body is plain data")
}

// ─── /stocks/summary ────────────────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Counts {
    in_stock_count: i64,
    out_of_stock_count: i64,
    low_stock_count: i64,
    total_quantity: i64,
    total_reserve: i64,
    total_value: f64,
    reserve_value: f64,
    average_value: f64,
    avg_data_points: i64,
}

#[derive(Serialize)]
struct StockRow {
    sku: Option<String>,
    quantity: Option<i64>,
    reserve: Option<i64>,
    price: f64,
    name: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StockSummary {
    summary: Counts,
    top_by_quantity: Vec<StockRow>,
    low_stock: Vec<StockRow>,
    last_sync: Option<String>,
}

fn stock_rows(rows: &[PgRow]) -> Result<Vec<StockRow>, Failure> {
    rows.iter()
        .map(|r| {
            Ok(StockRow {
                sku: text(r, 0)?,
                quantity: int(r, 1)?,
                reserve: int(r, 2)?,
                price: or_zero(f8(r, 3)?),
                name: text(r, 4)?,
            })
        })
        .collect()
}

async fn summary(store: &PgReads, limit: i64) -> Result<StockSummary, Failure> {
    let stats = read(
        store,
        format!(
            "SELECT COUNT(*) FILTER (WHERE quantity > 0), COUNT(*) FILTER (WHERE quantity = 0), \
             COUNT(*) FILTER (WHERE quantity > 0 AND quantity <= 5), \
             SUM(GREATEST(0, quantity - reserve)), SUM(reserve), \
             SUM(GREATEST(0, quantity - reserve) * price)::float8 AS available_value_f, \
             SUM(reserve * price)::float8 AS reserve_value_f, MAX(updated_at) \
             FROM {SKU_STATUS}"
        ),
        &[],
    )
    .await?;
    let top = read(
        store,
        format!(
            "SELECT sku, quantity, reserve, price::float8 AS price_f, name FROM {SKU_STATUS} \
             WHERE quantity > 0 ORDER BY quantity DESC, offer_id LIMIT $1"
        ),
        &[Bind::Int(limit)],
    )
    .await?;
    let low = read(
        store,
        format!(
            "SELECT sku, quantity, reserve, price::float8 AS price_f, name FROM {SKU_STATUS} \
             WHERE quantity > 0 AND quantity <= 5 ORDER BY quantity ASC, offer_id LIMIT 20"
        ),
        &[],
    )
    .await?;
    let average = read(
        store,
        format!(
            "WITH period_data AS (SELECT total_quantity, total_value, \
             ROW_NUMBER() OVER (ORDER BY date ASC) as rn_asc, \
             ROW_NUMBER() OVER (ORDER BY date DESC) as rn_desc \
             FROM {HISTORY} WHERE date >= {TODAY} - INTERVAL '30 days') \
             SELECT MAX(CASE WHEN rn_asc = 1 THEN total_quantity END), \
             MAX(CASE WHEN rn_asc = 1 THEN total_value END), \
             MAX(CASE WHEN rn_desc = 1 THEN total_quantity END), \
             MAX(CASE WHEN rn_desc = 1 THEN total_value END), COUNT(*) FROM period_data"
        ),
        &[],
    )
    .await?;

    let s = first(&stats)?;
    let available_value = or_zero(f8(s, 5)?);
    let snapshot_at: Option<DateTime<Utc>> = s
        .try_get(7)
        .map_err(|_| Failure::Raised("updated_at is not a timestamptz"))?;
    let a = first(&average)?;
    let gate = int(a, 0)?.is_some_and(|q| q != 0) && int(a, 2)?.is_some_and(|q| q != 0);
    let (average_value, avg_data_points) = if gate {
        // `(b or 0) + (e or 0)`, then `/ 2` and `round(…, 2)`: Decimal
        // arithmetic, unless both were falsy and the ints made a float.
        let begin = decimal(a, 1)?.filter(|d| !d.is_zero());
        let end = decimal(a, 3)?.filter(|d| !d.is_zero());
        let value = if begin.is_none() && end.is_none() {
            0.0
        } else {
            let sum = begin.unwrap_or(Decimal::ZERO) + end.unwrap_or(Decimal::ZERO);
            decimal_to_float(
                (sum / Decimal::TWO)
                    .round_dp_with_strategy(2, RoundingStrategy::MidpointNearestEven),
            )
        };
        (value, int(a, 4)?.unwrap_or(0))
    } else {
        (round_float(available_value, 2), 0)
    };
    Ok(StockSummary {
        summary: Counts {
            in_stock_count: int(s, 0)?.unwrap_or(0),
            out_of_stock_count: int(s, 1)?.unwrap_or(0),
            low_stock_count: int(s, 2)?.unwrap_or(0),
            total_quantity: int(s, 3)?.unwrap_or(0),
            total_reserve: int(s, 4)?.unwrap_or(0),
            total_value: available_value,
            reserve_value: or_zero(f8(s, 6)?),
            average_value,
            avg_data_points,
        },
        top_by_quantity: stock_rows(&top)?,
        low_stock: stock_rows(&low)?,
        last_sync: snapshot_at.map(isoformat),
    })
}

// ─── /stocks/average ────────────────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AverageFromSnapshot {
    average_quantity: i64,
    average_value: f64,
    beginning_quantity: Option<i64>,
    ending_quantity: i64,
    beginning_value: Option<f64>,
    ending_value: f64,
    data_points: i64,
    period_days: i64,
    message: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Average {
    average_quantity: i64,
    average_value: f64,
    beginning_quantity: i64,
    beginning_value: f64,
    beginning_date: Option<String>,
    ending_quantity: i64,
    ending_value: f64,
    ending_date: Option<String>,
    daily_average_quantity: i64,
    daily_average_value: f64,
    data_points: i64,
    period_days: i64,
}

async fn average(store: &PgReads, days: i64) -> Result<Vec<u8>, Failure> {
    let rows = read(
        store,
        format!(
            "WITH period_data AS (SELECT date, total_quantity, total_value, \
             ROW_NUMBER() OVER (ORDER BY date ASC) as rn_asc, \
             ROW_NUMBER() OVER (ORDER BY date DESC) as rn_desc \
             FROM {HISTORY} WHERE date >= {TODAY} - INTERVAL '{days} days') \
             SELECT MAX(CASE WHEN rn_asc = 1 THEN total_quantity END), \
             (MAX(CASE WHEN rn_asc = 1 THEN total_value END))::float8 AS beginning_value_f, \
             MAX(CASE WHEN rn_asc = 1 THEN date END), \
             MAX(CASE WHEN rn_desc = 1 THEN total_quantity END), \
             (MAX(CASE WHEN rn_desc = 1 THEN total_value END))::float8 AS ending_value_f, \
             MAX(CASE WHEN rn_desc = 1 THEN date END), \
             AVG(total_quantity)::float8 AS avg_daily_qty_f, \
             AVG(total_value)::float8 AS avg_daily_value_f, COUNT(*) FROM period_data"
        ),
        &[],
    )
    .await?;
    let r = first(&rows)?;
    let Some(beginning_qty) = int(r, 0)?.filter(|q| *q != 0) else {
        // No history: the current snapshot, a second round trip.
        let current = read(
            store,
            format!(
                "SELECT COALESCE(SUM(quantity - reserve), 0), \
                 COALESCE(SUM((quantity - reserve) * price), 0)::float8 AS value_f \
                 FROM {OFFER_STOCKS}"
            ),
            &[],
        )
        .await?;
        let c = first(&current)?;
        let quantity = int(c, 0)?.unwrap_or(0);
        let value = or_zero(f8(c, 1)?);
        return Ok(json(&AverageFromSnapshot {
            average_quantity: quantity,
            average_value: value,
            beginning_quantity: None,
            ending_quantity: quantity,
            beginning_value: None,
            ending_value: value,
            data_points: 0,
            period_days: days,
            message: "No historical data yet. Average based on current snapshot.",
        }));
    };
    let date = |i: usize| -> Result<Option<String>, Failure> {
        let d: Option<NaiveDate> = r
            .try_get(i)
            .map_err(|_| Failure::Raised("a date column is not a date"))?;
        Ok(d.map(|d| d.to_string()))
    };
    let beginning_value = or_zero(f8(r, 1)?);
    let ending_qty = int(r, 3)?.unwrap_or(0);
    let ending_value = or_zero(f8(r, 4)?);
    // `(b + e) / 2`: two ints make a float, exact below 2**53.
    let avg_qty = (beginning_qty + ending_qty) as f64 / 2.0;
    let avg_value = (beginning_value + ending_value) / 2.0;
    Ok(json(&Average {
        average_quantity: round_int(avg_qty)?,
        average_value: round_float(avg_value, 2),
        beginning_quantity: beginning_qty,
        beginning_value,
        beginning_date: date(2)?,
        ending_quantity: ending_qty,
        ending_value,
        ending_date: date(5)?,
        daily_average_quantity: round_int(or_zero(f8(r, 6)?))?,
        daily_average_value: round_float(or_zero(f8(r, 7)?), 2),
        data_points: int(r, 8)?.unwrap_or(0),
        period_days: days,
    }))
}

// ─── /stocks/trend ──────────────────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TrendMonthly {
    labels: Vec<String>,
    quantity: Vec<i64>,
    value: Vec<f64>,
    reserve: Vec<i64>,
    granularity: &'static str,
    period_days: i64,
    data_points: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TrendSummary {
    start_value: f64,
    end_value: f64,
    change: PyNum,
    change_percent: PyNum,
    min_value: f64,
    max_value: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TrendDaily {
    labels: Vec<String>,
    quantity: Vec<i64>,
    value: Vec<f64>,
    reserve: Vec<i64>,
    sku_count: Vec<i64>,
    value_change: Vec<PyNum>,
    granularity: &'static str,
    period_days: i64,
    data_points: usize,
    summary: Option<TrendSummary>,
}

/// `round(x or 0)` of an `AVG` read as its exact text.
fn round_avg(text: Option<String>) -> Result<i64, Failure> {
    text.map_or(Ok(0), |t| round_numeric(&t))
}

async fn trend(store: &PgReads, days: i64, granularity: &str) -> Result<Vec<u8>, Failure> {
    if granularity == "monthly" {
        // `DATE_TRUNC` of a date is a timestamptz, truncated in the
        // session's zone and handed over in UTC; `strftime` labels that.
        let rows = read(
            store,
            format!(
                "SELECT DATE_TRUNC('month', date) as period, \
                 AVG(total_quantity)::text AS avg_quantity_t, \
                 AVG(total_value)::float8 AS avg_value_f, \
                 AVG(total_reserve)::text AS avg_reserve_t \
                 FROM {HISTORY} WHERE date >= {TODAY} - INTERVAL '{days} days' \
                 GROUP BY DATE_TRUNC('month', date) ORDER BY period"
            ),
            &[],
        )
        .await?;
        let mut out = TrendMonthly {
            labels: Vec::new(),
            quantity: Vec::new(),
            value: Vec::new(),
            reserve: Vec::new(),
            granularity: "monthly",
            period_days: days,
            data_points: rows.len(),
        };
        for r in &rows {
            let period: Option<DateTime<Utc>> = r
                .try_get(0)
                .map_err(|_| Failure::Raised("the month is not a timestamptz"))?;
            if let Some(p) = period {
                out.labels.push(p.format("%b %Y").to_string());
            }
            out.quantity.push(round_avg(text(r, 1)?)?);
            out.value.push(round_float(or_zero(f8(r, 2)?), 2));
            out.reserve.push(round_avg(text(r, 3)?)?);
        }
        return Ok(json(&out));
    }
    let rows = read(
        store,
        format!(
            "SELECT date, total_quantity, total_value::float8 AS total_value_f, total_reserve, \
             sku_count FROM {HISTORY} WHERE date >= {TODAY} - INTERVAL '{days} days' ORDER BY date"
        ),
        &[],
    )
    .await?;
    let mut labels = Vec::new();
    let (mut quantity, mut values, mut reserve, mut sku_count) =
        (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for r in &rows {
        let day: Option<NaiveDate> = r
            .try_get(0)
            .map_err(|_| Failure::Raised("a date column is not a date"))?;
        if let Some(d) = day {
            labels.push(d.format("%d %b").to_string());
        }
        quantity.push(int(r, 1)?.unwrap_or(0));
        values.push(or_zero(f8(r, 2)?));
        reserve.push(int(r, 3)?.unwrap_or(0));
        sku_count.push(int(r, 4)?.unwrap_or(0));
    }
    let mut value_change = Vec::with_capacity(values.len());
    for (i, v) in values.iter().enumerate() {
        value_change.push(if i == 0 {
            PyNum::Int(0)
        } else {
            PyNum::Float(round_float(v - values[i - 1], 2))
        });
    }
    let summary = (!values.is_empty()).then(|| {
        let (start, end) = (values[0], values[values.len() - 1]);
        TrendSummary {
            start_value: start,
            end_value: end,
            change: if values.len() > 1 {
                PyNum::Float(round_float(end - start, 2))
            } else {
                PyNum::Int(0)
            },
            change_percent: if values.len() > 1 && start > 0.0 {
                PyNum::Float(round_float((end - start) / start * 100.0, 1))
            } else {
                PyNum::Int(0)
            },
            min_value: py_min(&values),
            max_value: py_max(&values),
        }
    });
    Ok(json(&TrendDaily {
        labels,
        quantity,
        data_points: rows.len(),
        value: values,
        reserve,
        sku_count,
        value_change,
        granularity: "daily",
        period_days: days,
        summary,
    }))
}

// ─── /stocks/analysis and /stocks/skus ──────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusEntry {
    sku_count: Option<i64>,
    quantity: i64,
    value: PyNum,
    value_percent: PyNum,
}

impl StatusEntry {
    /// `{"skuCount": 0, "quantity": 0, "value": 0, "valuePercent": 0}`.
    fn absent() -> Self {
        StatusEntry {
            sku_count: Some(0),
            quantity: 0,
            value: PyNum::Int(0),
            value_percent: PyNum::Int(0),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Total {
    sku_count: i64,
    quantity: i64,
    value: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatusSummary {
    healthy: StatusEntry,
    overstocked: StatusEntry,
    at_risk: StatusEntry,
    dead_stock: StatusEntry,
    never_sold: StatusEntry,
    total: Total,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Aging {
    bucket: Option<String>,
    sku_count: Option<i64>,
    units: Option<i64>,
    value: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Threshold {
    category_id: Option<i64>,
    category_name: String,
    sample_size: Option<i64>,
    p50: Option<i64>,
    p75: Option<i64>,
    p90: Option<i64>,
    threshold_days: i64,
}

/// `int(x) if x else None`: a NULL and a zero are None.
fn int_if(x: Option<f64>) -> Result<Option<i64>, Failure> {
    match x {
        Some(v) if v != 0.0 => trunc_int(v).map(Some),
        _ => Ok(None),
    }
}

/// `get_inventory_summary_v2`: the status cards, the aging buckets and the
/// per-category thresholds.
async fn summary_v2(
    store: &PgReads,
) -> Result<(StatusSummary, Vec<Aging>, Vec<Threshold>), Failure> {
    let statuses = read(
        store,
        "SELECT status, sku_count, total_units, total_value::float8 AS total_value_f, \
         value_pct::float8 AS value_pct_f FROM gold.v_inventory_summary"
            .to_owned(),
        &[],
    )
    .await?;
    let totals = read(
        store,
        "SELECT COUNT(*), SUM(available), SUM(available_value)::float8 AS available_value_f \
         FROM gold.v_sku_status"
            .to_owned(),
        &[],
    )
    .await?;
    let aging = read(
        store,
        "SELECT bucket, sku_count, units, value::float8 AS value_f FROM gold.v_aging_buckets \
         ORDER BY bucket"
            .to_owned(),
        &[],
    )
    .await?;
    let velocity = read(
        store,
        "SELECT category_id, category_name, sample_size, p50, p75, p90, threshold_days \
         FROM gold.v_category_velocity ORDER BY category_id NULLS LAST"
            .to_owned(),
        &[],
    )
    .await?;

    let mut by_status: Vec<(Option<String>, StatusEntry)> = Vec::new();
    for r in &statuses {
        let status = text(r, 0)?;
        let entry = StatusEntry {
            sku_count: int(r, 1)?,
            quantity: int(r, 2)?.unwrap_or(0),
            value: PyNum::Float(or_zero(f8(r, 3)?)),
            value_percent: PyNum::Float(or_zero(f8(r, 4)?)),
        };
        // `summary_dict[status] = …`: a repeat replaces the earlier one.
        match by_status.iter_mut().find(|(s, _)| *s == status) {
            Some((_, slot)) => *slot = entry,
            None => by_status.push((status, entry)),
        }
    }
    let mut take = |name: &str| {
        by_status
            .iter()
            .position(|(s, _)| s.as_deref() == Some(name))
            .map_or_else(StatusEntry::absent, |at| by_status.remove(at).1)
    };
    let t = first(&totals)?;
    let summary = StatusSummary {
        healthy: take("healthy"),
        overstocked: take("overstocked"),
        at_risk: take("at_risk"),
        dead_stock: take("dead_stock"),
        never_sold: take("never_sold"),
        total: Total {
            sku_count: int(t, 0)?.unwrap_or(0),
            quantity: int(t, 1)?.unwrap_or(0),
            value: or_zero(f8(t, 2)?),
        },
    };
    let aging = aging
        .iter()
        .map(|r| {
            Ok(Aging {
                bucket: text(r, 0)?,
                sku_count: int(r, 1)?,
                units: int(r, 2)?,
                value: or_zero(f8(r, 3)?),
            })
        })
        .collect::<Result<_, Failure>>()?;
    let thresholds = velocity
        .iter()
        .map(|r| {
            Ok(Threshold {
                category_id: int(r, 0)?,
                category_name: text(r, 1)?
                    .filter(|n| !n.is_empty())
                    .unwrap_or_else(|| "Uncategorized".to_owned()),
                sample_size: int(r, 2)?,
                p50: int_if(f8(r, 3)?)?,
                p75: int_if(f8(r, 4)?)?,
                p90: int_if(f8(r, 5)?)?,
                threshold_days: int_if(f8(r, 6)?)?.unwrap_or(180),
            })
        })
        .collect::<Result<_, Failure>>()?;
    Ok((summary, aging, thresholds))
}

/// One SKU of `get_dead_stock_deep`, with its NPV decision.
#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct Sku {
    offer_id: Option<i64>,
    sku: Option<String>,
    name: Option<String>,
    brand: Option<String>,
    category_name: Option<String>,
    abc_class: Option<String>,
    velocity_tier: Option<String>,
    units: i64,
    price: f64,
    purchased_price: Option<f64>,
    effective_unit_cost: f64,
    cost_quality: Option<String>,
    sale_value: f64,
    cost_basis: f64,
    excess_capital_cost: f64,
    days_since_sale: Option<i64>,
    days_in_stock: Option<i64>,
    days_of_supply: Option<i64>,
    qty_sold30d: i64,
    qty_sold90d: i64,
    revenue30d: f64,
    revenue90d: f64,
    avg_daily_sales30d: f64,
    avg_daily_sales90d: f64,
    velocity_ratio30to90: Option<f64>,
    annual_gross_profit: f64,
    gmroi: Option<f64>,
    npv_hold: f64,
    npv_liquidate: f64,
    decision: &'static str,
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct Cell {
    sku_count: i64,
    units: i64,
    cost_basis: f64,
    sale_value: f64,
    revenue90d: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Share {
    top_n: usize,
    excess_capital_cost: PyNum,
    share: PyNum,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Concentration {
    total_excess_capital_cost: PyNum,
    total_cost_basis: PyNum,
    top10: Share,
    top20: Share,
    top50: Share,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CostQuality {
    actual_skus: usize,
    fallback_skus: usize,
    actual_pct: PyNum,
    actual_cost_basis: PyNum,
    fallback_cost_basis: PyNum,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GmroiDistribution {
    median: PyNum,
    p25: PyNum,
    p75: PyNum,
    under100_count: usize,
    under100_cost_basis: PyNum,
    benchmark_range: [f64; 2],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Liquidation {
    sku_count: usize,
    cost_basis: PyNum,
    sale_value: PyNum,
    recovery_at_discount: PyNum,
    carrying_cost_saved_per_year: PyNum,
    discount: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Params {
    carrying_rate: f64,
    liquidation_discount: f64,
    optimal_days: i64,
}

struct Deep {
    /// Every SKU, by excess capital, the largest first.
    items: Vec<Sku>,
    matrix: Ordered<Ordered<Cell>>,
    concentration: Concentration,
    cost_quality: CostQuality,
    gmroi_distribution: GmroiDistribution,
    liquidation_summary: Liquidation,
    params: Params,
}

fn sum_of<'a>(items: impl IntoIterator<Item = &'a Sku>, f: impl Fn(&Sku) -> f64) -> PyNum {
    py_sum(&floats(items.into_iter().map(f)))
}

/// A row of `v_sku_dead_stock_v2` as Python holds it once the driver and
/// `float()` have been through it: each numeric the float Python makes of it.
#[derive(Clone, Debug, Default)]
pub struct DeepRow {
    pub offer_id: Option<i64>,
    pub sku: Option<String>,
    pub name: Option<String>,
    pub brand: Option<String>,
    pub category_name: Option<String>,
    pub available: Option<i64>,
    pub price: Option<f64>,
    pub purchased_price: Option<f64>,
    pub effective_unit_cost: Option<f64>,
    pub cost_quality: Option<String>,
    pub sale_value: Option<f64>,
    pub cost_basis: Option<f64>,
    pub days_since_sale: Option<i64>,
    pub days_in_stock: Option<i64>,
    pub qty_sold_30d: Option<i64>,
    pub qty_sold_90d: Option<i64>,
    pub revenue_30d: Option<f64>,
    pub revenue_90d: Option<f64>,
    pub avg_daily_sales_30d: Option<f64>,
    pub avg_daily_sales_90d: Option<f64>,
    pub days_of_supply: Option<f64>,
    pub velocity_tier: Option<String>,
    pub velocity_ratio_30_90: Option<f64>,
    pub abc_class: Option<String>,
    pub annual_gross_profit: Option<f64>,
    pub gmroi: Option<f64>,
}

fn deep_row(r: &PgRow) -> Result<DeepRow, Failure> {
    Ok(DeepRow {
        offer_id: int(r, 0)?,
        sku: text(r, 1)?,
        name: text(r, 2)?,
        brand: text(r, 3)?,
        category_name: text(r, 4)?,
        available: int(r, 5)?,
        price: f8(r, 6)?,
        purchased_price: f8(r, 7)?,
        effective_unit_cost: f8(r, 8)?,
        cost_quality: text(r, 9)?,
        sale_value: f8(r, 10)?,
        cost_basis: f8(r, 11)?,
        days_since_sale: int(r, 12)?,
        days_in_stock: int(r, 13)?,
        qty_sold_30d: int(r, 15)?,
        qty_sold_90d: int(r, 16)?,
        revenue_30d: f8(r, 17)?,
        revenue_90d: f8(r, 18)?,
        avg_daily_sales_30d: f8(r, 19)?,
        avg_daily_sales_90d: f8(r, 20)?,
        days_of_supply: f8(r, 21)?,
        velocity_tier: text(r, 22)?,
        velocity_ratio_30_90: f8(r, 23)?,
        abc_class: text(r, 24)?,
        annual_gross_profit: f8(r, 25)?,
        gmroi: f8(r, 26)?,
    })
}

/// One SKU, the decision taken as Python takes it.
fn sku_of(r: &DeepRow, carrying_rate: f64, discount: f64) -> Result<Sku, Failure> {
    let available = r.available.unwrap_or(0);
    let cost_basis = or_zero(r.cost_basis);
    let sale_value = or_zero(r.sale_value);
    let adv90 = or_zero(r.avg_daily_sales_90d);
    let dos = r.days_of_supply;
    let excess_units = max0(available as f64 - adv90 * OPTIMAL_DAYS as f64);
    let unit_cost = or_zero(r.effective_unit_cost);
    let excess_capital_cost = excess_units.as_f64() * unit_cost;
    let liq_revenue = sale_value * (1.0 - discount);
    let liq_npv = liq_revenue - cost_basis;
    let annual_gross_profit = or_zero(r.annual_gross_profit);
    let hold_npv = match dos {
        Some(d) if d <= 365.0 && adv90 > 0.0 => {
            // `min(dos, 365)` is `dos` here; `min(12.0, dos / 30.0)` keeps 12.0 on a tie.
            let hold_gross_profit = annual_gross_profit * (d / 365.0);
            let months = if d / 30.0 < 12.0 { d / 30.0 } else { 12.0 };
            let hold_carry_cost = cost_basis * carrying_rate * (months / 12.0) * 0.5;
            hold_gross_profit - hold_carry_cost
        }
        _ if adv90 > 0.0 => annual_gross_profit - cost_basis * carrying_rate,
        _ => -cost_basis * carrying_rate,
    };
    let tier = r.velocity_tier.as_deref();
    let decision = if matches!(tier, Some("frozen" | "cold")) && liq_npv > hold_npv {
        "LIQUIDATE"
    } else if matches!(tier, Some("frozen" | "cold"))
        || (tier == Some("warm") && dos.is_some_and(|d| d > 180.0))
    {
        "PROMO"
    } else {
        "HOLD"
    };
    Ok(Sku {
        offer_id: r.offer_id,
        sku: r.sku.clone(),
        name: r.name.clone(),
        brand: r.brand.clone(),
        category_name: r.category_name.clone(),
        abc_class: r.abc_class.clone(),
        velocity_tier: r.velocity_tier.clone(),
        units: available,
        price: or_zero(r.price),
        purchased_price: r.purchased_price.filter(|p| *p != 0.0),
        effective_unit_cost: unit_cost,
        cost_quality: r.cost_quality.clone(),
        sale_value,
        cost_basis,
        excess_capital_cost,
        days_since_sale: r.days_since_sale,
        days_in_stock: r.days_in_stock,
        days_of_supply: dos.map(trunc_int).transpose()?,
        qty_sold30d: r.qty_sold_30d.unwrap_or(0),
        qty_sold90d: r.qty_sold_90d.unwrap_or(0),
        revenue30d: or_zero(r.revenue_30d),
        revenue90d: or_zero(r.revenue_90d),
        avg_daily_sales30d: or_zero(r.avg_daily_sales_30d),
        avg_daily_sales90d: adv90,
        velocity_ratio30to90: r.velocity_ratio_30_90,
        annual_gross_profit,
        gmroi: r.gmroi,
        npv_hold: hold_npv,
        npv_liquidate: liq_npv,
        decision,
    })
}

/// `get_dead_stock_deep`: every SKU with its decision, and the aggregates.
async fn deep(store: &PgReads, carrying_rate: f64, discount: f64) -> Result<Deep, Failure> {
    let rows = read(
        store,
        "SELECT offer_id, sku, name, brand, category_name, available, \
         price::float8 AS price_f, purchased_price::float8 AS purchased_price_f, \
         effective_unit_cost::float8 AS effective_unit_cost_f, cost_quality, \
         sale_value::float8 AS sale_value_f, cost_basis::float8 AS cost_basis_f, \
         days_since_sale, days_in_stock, last_sale_date, qty_sold_30d, qty_sold_90d, \
         revenue_30d::float8 AS revenue_30d_f, revenue_90d::float8 AS revenue_90d_f, \
         avg_daily_sales_30d::float8 AS avg_daily_sales_30d_f, \
         avg_daily_sales_90d::float8 AS avg_daily_sales_90d_f, \
         days_of_supply::float8 AS days_of_supply_f, velocity_tier, \
         velocity_ratio_30_90::float8 AS velocity_ratio_30_90_f, abc_class, \
         annual_gross_profit::float8 AS annual_gross_profit_f, gmroi::float8 AS gmroi_f \
         FROM gold.v_sku_dead_stock_v2 ORDER BY offer_id"
            .to_owned(),
        &[],
    )
    .await?;
    let rows = rows
        .iter()
        .map(deep_row)
        .collect::<Result<Vec<_>, Failure>>()?;
    deep_of(&rows, carrying_rate, discount)
}

/// The decisions and the aggregates over rows already read.
fn deep_of(rows: &[DeepRow], carrying_rate: f64, discount: f64) -> Result<Deep, Failure> {
    let items_full = rows
        .iter()
        .map(|r| sku_of(r, carrying_rate, discount))
        .collect::<Result<Vec<_>, Failure>>()?;

    // The quadrant matrix, ABC × velocity tier, summed in offer order.
    let zero = Cell {
        sku_count: 0,
        units: 0,
        cost_basis: 0.0,
        sale_value: 0.0,
        revenue90d: 0.0,
    };
    let mut matrix = Ordered(
        ABCS.iter()
            .map(|a| (*a, Ordered(TIERS.iter().map(|t| (*t, zero)).collect())))
            .collect::<Vec<_>>(),
    );
    for it in &items_full {
        let row = matrix
            .0
            .iter_mut()
            .find(|(a, _)| Some(*a) == it.abc_class.as_deref())
            .ok_or(Failure::Raised(
                "KeyError: an ABC class the matrix has no row for",
            ))?;
        let cell = row
            .1
            .0
            .iter_mut()
            .find(|(t, _)| Some(*t) == it.velocity_tier.as_deref())
            .ok_or(Failure::Raised(
                "KeyError: a velocity tier the matrix has no column for",
            ))?;
        cell.1.sku_count += 1;
        cell.1.units += it.units;
        cell.1.cost_basis += it.cost_basis;
        cell.1.sale_value += it.sale_value;
        cell.1.revenue90d += it.revenue90d;
    }

    // `sorted(…, reverse=True)`: stable, so a tie keeps offer order.
    let mut by_excess = items_full.clone();
    by_excess.sort_by(|a, b| {
        b.excess_capital_cost
            .partial_cmp(&a.excess_capital_cost)
            .unwrap_or(Ordering::Equal)
    });
    let total_excess = sum_of(&items_full, |it| it.excess_capital_cost);
    let total_cost_basis = sum_of(&items_full, |it| it.cost_basis);
    let share = |n: usize| {
        let head_sum = sum_of(by_excess.iter().take(n), |it| it.excess_capital_cost);
        Share {
            top_n: n,
            excess_capital_cost: head_sum,
            share: if gt(total_excess, 0.0) {
                PyNum::Float(head_sum.as_f64() / total_excess.as_f64())
            } else {
                PyNum::Int(0)
            },
        }
    };
    let concentration = Concentration {
        total_excess_capital_cost: total_excess,
        total_cost_basis,
        top10: share(10),
        top20: share(20),
        top50: share(50),
    };

    let actual: Vec<&Sku> = items_full
        .iter()
        .filter(|it| it.cost_quality.as_deref() == Some("actual"))
        .collect();
    let actual_basis = sum_of(actual.iter().copied(), |it| it.cost_basis);
    let cost_quality = CostQuality {
        actual_skus: actual.len(),
        fallback_skus: items_full.len() - actual.len(),
        actual_pct: if items_full.is_empty() {
            PyNum::Int(0)
        } else {
            PyNum::Float(round_float(
                100.0 * actual.len() as f64 / items_full.len() as f64,
                1,
            ))
        },
        actual_cost_basis: actual_basis,
        fallback_cost_basis: minus(total_cost_basis, actual_basis),
    };

    let mut gmrois: Vec<f64> = items_full
        .iter()
        .filter_map(|it| it.gmroi.filter(|g| *g > 0.0))
        .collect();
    gmrois.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let (median, p25, p75) = if gmrois.is_empty() {
        (PyNum::Int(0), PyNum::Int(0), PyNum::Int(0))
    } else {
        let n = gmrois.len();
        (
            PyNum::Float(gmrois[n / 2]),
            PyNum::Float(gmrois[n / 4]),
            PyNum::Float(gmrois[(3 * n) / 4]),
        )
    };
    let under_100: Vec<&Sku> = items_full
        .iter()
        .filter(|it| it.gmroi.is_some_and(|g| g < 1.0) && it.cost_basis > 0.0)
        .collect();
    let gmroi_distribution = GmroiDistribution {
        median,
        p25,
        p75,
        under100_count: under_100.len(),
        under100_cost_basis: sum_of(under_100.iter().copied(), |it| it.cost_basis),
        benchmark_range: [2.0, 4.0],
    };

    let liq: Vec<&Sku> = items_full
        .iter()
        .filter(|it| it.decision == "LIQUIDATE")
        .collect();
    let liquidation_summary = Liquidation {
        sku_count: liq.len(),
        cost_basis: sum_of(liq.iter().copied(), |it| it.cost_basis),
        sale_value: sum_of(liq.iter().copied(), |it| it.sale_value),
        recovery_at_discount: sum_of(liq.iter().copied(), |it| it.sale_value * (1.0 - discount)),
        carrying_cost_saved_per_year: sum_of(liq.iter().copied(), |it| {
            it.cost_basis * carrying_rate
        }),
        discount,
    };
    Ok(Deep {
        items: by_excess,
        matrix,
        concentration,
        cost_quality,
        gmroi_distribution,
        liquidation_summary,
        params: Params {
            carrying_rate,
            liquidation_discount: discount,
            optimal_days: OPTIMAL_DAYS,
        },
    })
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeepJson<'a> {
    items: &'a [Sku],
    quadrant_matrix: &'a Ordered<Ordered<Cell>>,
    concentration: &'a Concentration,
    cost_quality: &'a CostQuality,
    gmroi_distribution: &'a GmroiDistribution,
    liquidation_summary: &'a Liquidation,
    params: &'a Params,
}

/// `get_dead_stock_deep(limit, carrying_rate, liquidation_discount)` over
/// rows already read, as orjson writes the dict it returns.
pub fn deep_json(
    rows: &[DeepRow],
    carrying_rate: f64,
    discount: f64,
    limit: Option<usize>,
) -> Result<Vec<u8>, Failure> {
    let deep = deep_of(rows, carrying_rate, discount)?;
    let items = match limit {
        Some(n) => &deep.items[..deep.items.len().min(n)],
        None => &deep.items[..],
    };
    Ok(json(&DeepJson {
        items,
        quadrant_matrix: &deep.matrix,
        concentration: &deep.concentration,
        cost_quality: &deep.cost_quality,
        gmroi_distribution: &deep.gmroi_distribution,
        liquidation_summary: &deep.liquidation_summary,
        params: &deep.params,
    }))
}

#[derive(Serialize)]
struct VelocityTiers {
    hot: &'static str,
    healthy: &'static str,
    warm: &'static str,
    cold: &'static str,
    frozen: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Methodology {
    description: &'static str,
    minimum_threshold: i64,
    default_threshold: i64,
    at_risk_multiplier: f64,
    velocity_tiers: VelocityTiers,
    optimal_days: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Analysis<'a> {
    summary: StatusSummary,
    aging_buckets: Vec<Aging>,
    category_thresholds: Vec<Threshold>,
    items: &'a [Sku],
    quadrant_matrix: &'a Ordered<Ordered<Cell>>,
    concentration: &'a Concentration,
    cost_quality: &'a CostQuality,
    gmroi_distribution: &'a GmroiDistribution,
    liquidation_summary: &'a Liquidation,
    params: &'a Params,
    methodology: Methodology,
}

async fn analysis(store: &PgReads, carrying_rate: f64, discount: f64) -> Result<Vec<u8>, Failure> {
    let (summary, aging_buckets, category_thresholds) = summary_v2(store).await?;
    let deep = deep(store, carrying_rate, discount).await?;
    Ok(json(&Analysis {
        summary,
        aging_buckets,
        category_thresholds,
        items: &deep.items[..deep.items.len().min(100)],
        quadrant_matrix: &deep.matrix,
        concentration: &deep.concentration,
        cost_quality: &deep.cost_quality,
        gmroi_distribution: &deep.gmroi_distribution,
        liquidation_summary: &deep.liquidation_summary,
        params: &deep.params,
        methodology: Methodology {
            description: "Dynamic thresholds per category using P75, minimum 90 days, \
                          maximum 365 days",
            minimum_threshold: 90,
            default_threshold: 180,
            at_risk_multiplier: 0.7,
            velocity_tiers: VelocityTiers {
                hot: "DOS ≤ 30",
                healthy: "30 < DOS ≤ 90",
                warm: "90 < DOS ≤ 180",
                cold: "180 < DOS ≤ 365",
                frozen: "DOS > 365 or no sales in 90d",
            },
            optimal_days: deep.params.optimal_days,
        },
    }))
}

// ─── /stocks/turnover ───────────────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TurnoverPeriod {
    period_days: i64,
    total_revenue: f64,
    actual_days: i64,
    daily_revenue: f64,
    monthly_revenue: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CurrentStock {
    value_sale: f64,
    value_cost: f64,
    units: i64,
    active_skus: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Benchmarks {
    dsi: [i64; 2],
    turnover_ratio: [i64; 2],
    stock_to_sales: [f64; 2],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Kpis {
    dsi: PyNum,
    turnover_ratio: PyNum,
    stock_to_sales: PyNum,
    benchmarks: Benchmarks,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Optimal {
    lead_time_days: i64,
    safety_days: i64,
    buffer_days: i64,
    total_days: i64,
    total_value: f64,
    max_acceptable_days: i64,
    max_acceptable_value: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Excess {
    excess_value: PyNum,
    excess_ratio: PyNum,
    excess_days: PyNum,
    carrying_cost_annual: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SellThrough {
    fast_movers: i64,
    slow_movers: i64,
    zero_velocity: i64,
    avg_sell_through_rate: f64,
    median_days_of_supply: Option<f64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AbcEntry {
    sku_count: i64,
    total_units: i64,
    stock_value: PyNum,
    revenue: PyNum,
    stock_pct: PyNum,
    revenue_pct: PyNum,
}

impl AbcEntry {
    fn absent() -> Self {
        AbcEntry {
            sku_count: 0,
            total_units: 0,
            stock_value: PyNum::Int(0),
            revenue: PyNum::Int(0),
            stock_pct: PyNum::Int(0),
            revenue_pct: PyNum::Int(0),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AbcBlock {
    #[serde(rename = "A")]
    a: AbcEntry,
    #[serde(rename = "B")]
    b: AbcEntry,
    #[serde(rename = "C")]
    c: AbcEntry,
    imbalance_score: PyNum,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TopExcess {
    offer_id: Option<i64>,
    sku: Option<String>,
    name: Option<String>,
    brand: Option<String>,
    category_name: Option<String>,
    units: i64,
    value: f64,
    days_of_supply: Option<i64>,
    sell_through_rate: f64,
    avg_daily_sales: f64,
    revenue90d: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Turnover {
    turnover: TurnoverPeriod,
    current_stock: CurrentStock,
    kpis: Kpis,
    optimal: Optimal,
    excess: Excess,
    sell_through: SellThrough,
    abc: AbcBlock,
    top_excess: Vec<TopExcess>,
}

/// `round(x, n) if cond else 0`.
fn rounded_if(cond: bool, x: impl FnOnce() -> f64, n: usize) -> PyNum {
    if cond {
        PyNum::Float(round_float(x(), n))
    } else {
        PyNum::Int(0)
    }
}

/// One row of `v_abc_summary` as Python holds it.
#[derive(Clone, Debug, Default)]
pub struct AbcRow {
    pub class: Option<String>,
    pub sku_count: Option<i64>,
    pub total_units: Option<i64>,
    pub stock_value: Option<f64>,
    pub revenue: Option<f64>,
    pub stock_pct: Option<f64>,
    pub revenue_pct: Option<f64>,
}

/// One of the top excess SKUs; `days_of_supply` is the numeric's exact
/// text, because `int()` cuts the Decimal and not a float made of it.
#[derive(Clone, Debug, Default)]
pub struct ExcessRow {
    pub offer_id: Option<i64>,
    pub sku: Option<String>,
    pub name: Option<String>,
    pub brand: Option<String>,
    pub category_name: Option<String>,
    pub available: Option<i64>,
    pub value: Option<f64>,
    pub days_of_supply: Option<String>,
    pub sell_through_rate: Option<f64>,
    pub avg_daily_sales: Option<f64>,
    pub revenue_90d: Option<f64>,
}

/// The five reads `get_inventory_turnover` combines.
#[derive(Clone, Debug, Default)]
pub struct TurnoverRows {
    /// `rev`: `float(SUM(revenue))` and the days that had any.
    pub revenue: Option<f64>,
    pub revenue_days: Option<i64>,
    /// `stock`: the value at sale and at cost, the units, the SKUs in stock.
    pub stock_sale: Option<f64>,
    pub stock_cost: Option<f64>,
    pub stock_units: Option<i64>,
    pub active_skus: Option<i64>,
    pub abc: Vec<AbcRow>,
    /// `st`: fast, slow and zero movers, the average rate, the median days.
    pub fast: Option<i64>,
    pub slow: Option<i64>,
    pub zero: Option<i64>,
    pub avg_rate: Option<f64>,
    pub median_days: Option<f64>,
    pub excess: Vec<ExcessRow>,
}

/// The route's five settings.
#[derive(Clone, Copy, Debug)]
pub struct TurnoverParams {
    pub days: i64,
    pub lead_time: i64,
    pub safety_multiplier: f64,
    pub buffer_days: i64,
    pub max_acceptable_days: i64,
}

async fn turnover(store: &PgReads, q: &Query) -> Result<Vec<u8>, Failure> {
    let days = q.days;
    let rev = read(
        store,
        format!(
            "SELECT COALESCE(SUM(revenue), 0)::float8 AS revenue_f, COUNT(DISTINCT date) \
             FROM gold.daily_revenue WHERE date >= {TODAY} - INTERVAL '{days} days' \
             AND source_id IS NULL"
        ),
        &[],
    )
    .await?;
    let stock = read(
        store,
        format!(
            "SELECT COALESCE(SUM(GREATEST(0, quantity - reserve) * price), 0)::float8 AS sale_f, \
             COALESCE(SUM(GREATEST(0, quantity - reserve) * COALESCE(purchased_price, 0)), 0)\
             ::float8 AS cost_f, COALESCE(SUM(GREATEST(0, quantity - reserve)), 0), \
             COUNT(*) FILTER (WHERE quantity > 0) FROM {OFFER_STOCKS}"
        ),
        &[],
    )
    .await?;
    let abc = read(
        store,
        "SELECT abc_class, sku_count, total_units, stock_value::float8 AS stock_value_f, \
         revenue::float8 AS revenue_f, stock_value_pct::float8 AS stock_value_pct_f, \
         revenue_pct::float8 AS revenue_pct_f FROM gold.v_abc_summary"
            .to_owned(),
        &[],
    )
    .await?;
    let st = read(
        store,
        "SELECT COUNT(*) FILTER (WHERE sell_through_rate_30d >= 20), \
         COUNT(*) FILTER (WHERE sell_through_rate_30d > 0 AND sell_through_rate_30d < 20), \
         COUNT(*) FILTER (WHERE sell_through_rate_30d = 0 AND available > 0), \
         (AVG(sell_through_rate_30d) FILTER (WHERE sell_through_rate_30d > 0))::float8 \
         AS avg_rate_f, \
         PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY days_of_supply) \
         FILTER (WHERE days_of_supply IS NOT NULL) FROM gold.v_sku_sell_through"
            .to_owned(),
        &[],
    )
    .await?;
    let excess = read(
        store,
        "SELECT offer_id, sku, name, brand, category_name, available, \
         available_value::float8 AS available_value_f, days_of_supply::text AS days_of_supply_t, \
         sell_through_rate_30d::float8 AS sell_through_rate_30d_f, \
         avg_daily_sales::float8 AS avg_daily_sales_f, revenue_90d::float8 AS revenue_90d_f \
         FROM gold.v_sku_sell_through \
         WHERE days_of_supply > 90 OR (avg_daily_sales = 0 AND available > 0) \
         ORDER BY available_value DESC, offer_id LIMIT 20"
            .to_owned(),
        &[],
    )
    .await?;
    let (r, s, t) = (first(&rev)?, first(&stock)?, first(&st)?);
    let rows = TurnoverRows {
        revenue: f8(r, 0)?,
        revenue_days: int(r, 1)?,
        stock_sale: f8(s, 0)?,
        stock_cost: f8(s, 1)?,
        stock_units: int(s, 2)?,
        active_skus: int(s, 3)?,
        abc: abc
            .iter()
            .map(|row| {
                Ok(AbcRow {
                    class: text(row, 0)?,
                    sku_count: int(row, 1)?,
                    total_units: int(row, 2)?,
                    stock_value: f8(row, 3)?,
                    revenue: f8(row, 4)?,
                    stock_pct: f8(row, 5)?,
                    revenue_pct: f8(row, 6)?,
                })
            })
            .collect::<Result<_, Failure>>()?,
        fast: int(t, 0)?,
        slow: int(t, 1)?,
        zero: int(t, 2)?,
        avg_rate: f8(t, 3)?,
        median_days: f8(t, 4)?,
        excess: excess
            .iter()
            .map(|row| {
                Ok(ExcessRow {
                    offer_id: int(row, 0)?,
                    sku: text(row, 1)?,
                    name: text(row, 2)?,
                    brand: text(row, 3)?,
                    category_name: text(row, 4)?,
                    available: int(row, 5)?,
                    value: f8(row, 6)?,
                    days_of_supply: text(row, 7)?,
                    sell_through_rate: f8(row, 8)?,
                    avg_daily_sales: f8(row, 9)?,
                    revenue_90d: f8(row, 10)?,
                })
            })
            .collect::<Result<_, Failure>>()?,
    };
    turnover_json(
        &rows,
        TurnoverParams {
            days,
            lead_time: q.lead_time,
            safety_multiplier: q.safety_multiplier,
            buffer_days: q.buffer_days,
            max_acceptable_days: q.max_acceptable_days,
        },
    )
}

/// `get_inventory_turnover`'s arithmetic over its five reads, as orjson
/// writes the answer.
pub fn turnover_json(rows: &TurnoverRows, p: TurnoverParams) -> Result<Vec<u8>, Failure> {
    let total_revenue = rows.revenue.ok_or(Failure::Raised("float(None)"))?;
    let actual_days = rows.revenue_days.filter(|d| *d != 0).unwrap_or(1);
    let stock_value_sale = rows.stock_sale.ok_or(Failure::Raised("float(None)"))?;
    let stock_value_cost = rows.stock_cost.ok_or(Failure::Raised("float(None)"))?;
    let stock_units = rows.stock_units.ok_or(Failure::Raised("int(None)"))?;
    let active_skus = rows.active_skus.ok_or(Failure::Raised("int(None)"))?;

    let daily_revenue = total_revenue / actual_days as f64;
    let monthly_revenue = daily_revenue * 30.0;
    let dsi = rounded_if(daily_revenue > 0.0, || stock_value_sale / daily_revenue, 1);
    let turnover_ratio = rounded_if(
        stock_value_sale > 0.0,
        || (daily_revenue * 365.0) / stock_value_sale,
        2,
    );
    let stock_to_sales = rounded_if(
        monthly_revenue > 0.0,
        || stock_value_sale / monthly_revenue,
        2,
    );

    let safety_days = round_int(p.lead_time as f64 * (p.safety_multiplier - 1.0))?;
    let optimal_days = p.lead_time + safety_days + p.buffer_days;
    let optimal_value = daily_revenue * optimal_days as f64;
    let max_acceptable_value = daily_revenue * p.max_acceptable_days as f64;
    let excess_value = max0(stock_value_sale - optimal_value);
    let excess_ratio = rounded_if(optimal_value > 0.0, || stock_value_sale / optimal_value, 2);
    let excess_days = rounded_if(
        gt(dsi, optimal_days as f64),
        || dsi.as_f64() - optimal_days as f64,
        1,
    );
    let carrying_cost_annual = round_float(excess_value.as_f64() * 0.25, 2);

    let (mut a, mut b, mut c) = (None, None, None);
    let (mut c_stock_pct, mut c_revenue_pct) = (0.0, 0.0);
    for row in &rows.abc {
        let entry = AbcEntry {
            sku_count: row.sku_count.ok_or(Failure::Raised("int(None)"))?,
            total_units: row.total_units.unwrap_or(0),
            stock_value: PyNum::Float(or_zero(row.stock_value)),
            revenue: PyNum::Float(or_zero(row.revenue)),
            stock_pct: PyNum::Float(or_zero(row.stock_pct)),
            revenue_pct: PyNum::Float(or_zero(row.revenue_pct)),
        };
        match row.class.as_deref() {
            Some("A") => a = Some(entry),
            Some("B") => b = Some(entry),
            Some("C") => {
                c_stock_pct = entry.stock_pct.as_f64();
                c_revenue_pct = entry.revenue_pct.as_f64();
                c = Some(entry);
            }
            _ => {}
        }
    }
    let top_excess = rows
        .excess
        .iter()
        .map(|r| {
            Ok(TopExcess {
                offer_id: r.offer_id,
                sku: r.sku.clone(),
                name: r.name.clone(),
                brand: r.brand.clone(),
                category_name: r.category_name.clone(),
                units: r.available.unwrap_or(0),
                value: or_zero(r.value),
                days_of_supply: r.days_of_supply.as_deref().map(trunc_numeric).transpose()?,
                sell_through_rate: or_zero(r.sell_through_rate),
                avg_daily_sales: or_zero(r.avg_daily_sales),
                revenue90d: or_zero(r.revenue_90d),
            })
        })
        .collect::<Result<_, Failure>>()?;
    Ok(json(&Turnover {
        turnover: TurnoverPeriod {
            period_days: p.days,
            total_revenue: round_float(total_revenue, 2),
            actual_days,
            daily_revenue: round_float(daily_revenue, 2),
            monthly_revenue: round_float(monthly_revenue, 2),
        },
        current_stock: CurrentStock {
            value_sale: round_float(stock_value_sale, 2),
            value_cost: round_float(stock_value_cost, 2),
            units: stock_units,
            active_skus,
        },
        kpis: Kpis {
            dsi,
            turnover_ratio,
            stock_to_sales,
            benchmarks: Benchmarks {
                dsi: [60, 90],
                turnover_ratio: [4, 6],
                stock_to_sales: [1.0, 2.5],
            },
        },
        optimal: Optimal {
            lead_time_days: p.lead_time,
            safety_days,
            buffer_days: p.buffer_days,
            total_days: optimal_days,
            total_value: round_float(optimal_value, 2),
            max_acceptable_days: p.max_acceptable_days,
            max_acceptable_value: round_float(max_acceptable_value, 2),
        },
        excess: Excess {
            excess_value: round_num(excess_value, 2),
            excess_ratio,
            excess_days,
            carrying_cost_annual,
        },
        sell_through: SellThrough {
            fast_movers: rows.fast.unwrap_or(0),
            slow_movers: rows.slow.unwrap_or(0),
            zero_velocity: rows.zero.unwrap_or(0),
            avg_sell_through_rate: round_float(or_zero(rows.avg_rate), 1),
            median_days_of_supply: rows.median_days.map(|m| round_float(or_zero(Some(m)), 0)),
        },
        abc: AbcBlock {
            a: a.unwrap_or_else(AbcEntry::absent),
            b: b.unwrap_or_else(AbcEntry::absent),
            c: c.unwrap_or_else(AbcEntry::absent),
            imbalance_score: rounded_if(c_revenue_pct > 0.0, || c_stock_pct / c_revenue_pct, 2),
        },
        top_excess,
    }))
}

// ─── /stocks/brand-rotation ─────────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Brand {
    brand: Option<String>,
    sku_count: Option<i64>,
    frozen_skus: i64,
    frozen_share: PyNum,
    units: i64,
    cost_basis: f64,
    sale_value: f64,
    revenue90d: f64,
    qty_sold90d: PyNum,
    annual_gross_profit: f64,
    rotation_days: Option<i64>,
    gmroi: Option<f64>,
    health: &'static str,
}

/// One brand of the scorecard's GROUP BY as Python holds it; the 90 days'
/// quantity is the numeric's text, handed on as FastAPI's encoder writes it.
#[derive(Clone, Debug, Default)]
pub struct BrandRow {
    pub brand: Option<String>,
    pub sku_count: Option<i64>,
    pub frozen_skus: Option<i64>,
    pub units: Option<i64>,
    pub cost_basis: Option<f64>,
    pub sale_value: Option<f64>,
    pub revenue_90d: Option<f64>,
    pub qty_sold_90d: Option<String>,
    pub annual_gross_profit: Option<f64>,
}

async fn brand_rotation(store: &PgReads, min_skus: i64) -> Result<Vec<u8>, Failure> {
    // `ORDER BY cost_basis` names the output column, `SUM(cost_basis)`: the
    // exact sum sorts, the float8 beside it is only what Python reads.
    let rows = read(
        store,
        "SELECT COALESCE(NULLIF(brand, ''), '—') as brand, COUNT(*) as sku_count, \
         SUM(CASE WHEN velocity_tier IN ('frozen','cold') THEN 1 ELSE 0 END) as frozen_skus, \
         SUM(available) as units, SUM(cost_basis)::float8 AS cost_basis_f, \
         SUM(sale_value)::float8 AS sale_value_f, SUM(revenue_90d)::float8 AS revenue_90d_f, \
         SUM(qty_sold_90d)::text AS qty_sold_90d_t, \
         SUM(annual_gross_profit)::float8 AS annual_gross_profit_f \
         FROM gold.v_sku_dead_stock_v2 GROUP BY 1 HAVING COUNT(*) >= $1 \
         ORDER BY SUM(cost_basis) DESC, 1"
            .to_owned(),
        &[Bind::Int(min_skus)],
    )
    .await?;
    let rows = rows
        .iter()
        .map(|r| {
            Ok(BrandRow {
                brand: text(r, 0)?,
                sku_count: int(r, 1)?,
                frozen_skus: int(r, 2)?,
                units: int(r, 3)?,
                cost_basis: f8(r, 4)?,
                sale_value: f8(r, 5)?,
                revenue_90d: f8(r, 6)?,
                qty_sold_90d: text(r, 7)?,
                annual_gross_profit: f8(r, 8)?,
            })
        })
        .collect::<Result<Vec<_>, Failure>>()?;
    brands_json(&rows)
}

/// `get_brand_rotation`'s scorecard over its rows, as orjson writes it.
pub fn brands_json(rows: &[BrandRow]) -> Result<Vec<u8>, Failure> {
    let brands = rows
        .iter()
        .map(|r| {
            let cost_basis = or_zero(r.cost_basis);
            let sale_value = or_zero(r.sale_value);
            let revenue_90d = or_zero(r.revenue_90d);
            let gross_profit = or_zero(r.annual_gross_profit);
            let cogs_90d = if sale_value > 0.0 {
                revenue_90d * (cost_basis / sale_value)
            } else {
                0.0
            };
            let rotation_days = (cogs_90d > 0.0).then(|| cost_basis / (cogs_90d / 90.0));
            let health = match rotation_days {
                None => "critical",
                Some(d) if d > 365.0 => "critical",
                Some(d) if d > 200.0 => "poor",
                Some(d) if d > 120.0 => "warning",
                Some(d) if d > 60.0 => "ok",
                Some(_) => "great",
            };
            let frozen_share = match r.sku_count {
                Some(n) if n != 0 => PyNum::Float(
                    r.frozen_skus.ok_or(Failure::Raised("None / int"))? as f64 / n as f64,
                ),
                _ => PyNum::Int(0),
            };
            Ok(Brand {
                brand: r.brand.clone(),
                sku_count: r.sku_count,
                frozen_skus: r.frozen_skus.unwrap_or(0),
                frozen_share,
                units: r.units.unwrap_or(0),
                cost_basis,
                sale_value,
                revenue90d: revenue_90d,
                qty_sold90d: numeric_or_zero(r.qty_sold_90d.clone())?,
                annual_gross_profit: gross_profit,
                rotation_days: rotation_days.map(trunc_int).transpose()?,
                gmroi: (cost_basis > 0.0).then(|| gross_profit / cost_basis),
                health,
            })
        })
        .collect::<Result<Vec<_>, Failure>>()?;
    Ok(json(&brands))
}

// ─── /stocks/abc/{abc_class}, /stocks/actions, /stocks/alerts ───────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AbcSku {
    offer_id: Option<i64>,
    sku: Option<String>,
    name: Option<String>,
    brand: Option<String>,
    category_name: Option<String>,
    units: i64,
    value: f64,
    price: f64,
    revenue90d: f64,
    qty_sold90d: i64,
}

async fn abc_skus(store: &PgReads, class: &str, limit: i64) -> Result<Vec<u8>, Failure> {
    let rows = read(
        store,
        "SELECT offer_id, sku, name, brand, category_name, available, \
         available_value::float8 AS available_value_f, price::float8 AS price_f, \
         revenue_90d::float8 AS revenue_90d_f, qty_sold_90d \
         FROM gold.v_abc_classification WHERE abc_class = $1 \
         ORDER BY revenue_90d DESC, offer_id LIMIT $2"
            .to_owned(),
        &[Bind::Text(class.to_owned()), Bind::Int(limit)],
    )
    .await?;
    let skus = rows
        .iter()
        .map(|r| {
            Ok(AbcSku {
                offer_id: int(r, 0)?,
                sku: text(r, 1)?,
                name: text(r, 2)?,
                brand: text(r, 3)?,
                category_name: text(r, 4)?,
                units: int(r, 5)?.unwrap_or(0),
                value: or_zero(f8(r, 6)?),
                price: or_zero(f8(r, 7)?),
                revenue90d: or_zero(f8(r, 8)?),
                qty_sold90d: int(r, 9)?.unwrap_or(0),
            })
        })
        .collect::<Result<Vec<_>, Failure>>()?;
    Ok(json(&skus))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Action {
    offer_id: Option<i64>,
    sku: Option<String>,
    name: Option<String>,
    brand: Option<String>,
    category_name: Option<String>,
    units: Option<i64>,
    value: f64,
    days_since_sale: Option<i64>,
    days_in_stock: Option<i64>,
    status: Option<String>,
    action: Option<String>,
}

async fn actions(store: &PgReads) -> Result<Vec<u8>, Failure> {
    let rows = read(
        store,
        "SELECT offer_id, sku, name, brand, category_name, units, value::float8 AS value_f, \
         days_since_sale, days_in_stock, status, action FROM gold.v_recommended_actions \
         WHERE action IS NOT NULL ORDER BY value DESC, offer_id LIMIT $1"
            .to_owned(),
        &[Bind::Int(50)],
    )
    .await?;
    let out = rows
        .iter()
        .map(|r| {
            Ok(Action {
                offer_id: int(r, 0)?,
                sku: text(r, 1)?,
                name: text(r, 2)?,
                brand: text(r, 3)?,
                category_name: text(r, 4)?,
                units: int(r, 5)?,
                value: or_zero(f8(r, 6)?),
                days_since_sale: int(r, 7)?,
                days_in_stock: int(r, 8)?,
                status: text(r, 9)?,
                action: text(r, 10)?,
            })
        })
        .collect::<Result<Vec<_>, Failure>>()?;
    Ok(json(&out))
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Alert {
    offer_id: Option<i64>,
    sku: Option<String>,
    name: Option<String>,
    brand: Option<String>,
    units_left: Option<i64>,
    days_since_sale: Option<i64>,
    alert_level: Option<String>,
}

async fn alerts(store: &PgReads) -> Result<Vec<u8>, Failure> {
    let rows = read(
        store,
        "SELECT offer_id, sku, name, brand, units_left, days_since_sale, alert_level \
         FROM gold.v_restock_alerts WHERE alert_level IS NOT NULL \
         ORDER BY units_left, offer_id LIMIT $1"
            .to_owned(),
        &[Bind::Int(50)],
    )
    .await?;
    let out = rows
        .iter()
        .map(|r| {
            Ok(Alert {
                offer_id: int(r, 0)?,
                sku: text(r, 1)?,
                name: text(r, 2)?,
                brand: text(r, 3)?,
                units_left: int(r, 4)?,
                days_since_sale: int(r, 5)?,
                alert_level: text(r, 6)?,
            })
        })
        .collect::<Result<Vec<_>, Failure>>()?;
    Ok(json(&out))
}

// ─── The entry point ────────────────────────────────────────────────────────

/// The JSON body for a 200, or the reason there is none. `abc_class` is the
/// path's segment; `admit` is the route's rate limit, which slowapi checks
/// once the parameters parse and before the route's own checks run.
pub async fn answer(
    route: Route,
    store: &PgReads,
    raw_query: &str,
    abc_class: Option<&str>,
    admit: impl FnOnce() -> Result<(), Failure>,
) -> Result<Vec<u8>, Failure> {
    let q = parse(route, raw_query)?;
    admit()?;
    match route {
        Route::Summary => Ok(json(&summary(store, q.limit).await?)),
        Route::Average => average(store, q.days).await,
        Route::Trend => {
            // `if granularity not in ("daily", "monthly"): granularity = "daily"`.
            let granularity = if q.granularity == "monthly" {
                "monthly"
            } else {
                "daily"
            };
            trend(store, q.days, granularity).await
        }
        Route::Analysis => analysis(store, q.carrying_rate, q.liquidation_discount).await,
        Route::Skus => Ok(json(
            &deep(store, q.carrying_rate, q.liquidation_discount)
                .await?
                .items,
        )),
        Route::Turnover => turnover(store, &q).await,
        Route::BrandRotation => brand_rotation(store, q.min_skus).await,
        Route::Abc => {
            // `abc_class.upper() in ("A", "B", "C")`: only these six letters
            // upper-case into one of the three.
            let class = match abc_class.unwrap_or("") {
                "A" | "a" => "A",
                "B" | "b" => "B",
                "C" | "c" => "C",
                _ => {
                    return Err(Failure::BadRequest(
                        "abc_class must be A, B, or C".to_owned(),
                    ));
                }
            };
            abc_skus(store, class, q.limit).await
        }
        Route::Actions => actions(store).await,
        Route::Alerts => alerts(store).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(route: Route, query: &str) -> Vec<(&'static str, &'static str)> {
        match parse(route, query) {
            Err(Failure::Unprocessable(errors)) => {
                errors.iter().map(|e| (e.kind, e.loc.1)).collect()
            }
            Err(other) => panic!("{other:?}"),
            Ok(_) => vec![],
        }
    }

    #[test]
    fn each_signature_in_its_order() {
        assert_eq!(
            kinds(
                Route::Turnover,
                "max_acceptable_days=1&buffer_days=x&safety_multiplier=4&lead_time=0&days=6"
            ),
            vec![
                ("greater_than_equal", "days"),
                ("greater_than_equal", "lead_time"),
                ("less_than_equal", "safety_multiplier"),
                ("int_parsing", "buffer_days"),
                ("greater_than_equal", "max_acceptable_days"),
            ]
        );
        assert_eq!(
            kinds(
                Route::Analysis,
                "liquidation_discount=nan&carrying_rate=inf"
            ),
            vec![
                ("less_than_equal", "carrying_rate"),
                ("greater_than_equal", "liquidation_discount"),
            ]
        );
        assert!(kinds(Route::Actions, "limit=x").is_empty());
    }

    #[test]
    fn floats_as_pydantic_reads_them() {
        assert_eq!(lax_float(" 0.25 "), Some(0.25));
        assert_eq!(lax_float("2_5e-2"), Some(0.25));
        assert_eq!(lax_float("_1"), None);
        assert_eq!(lax_float("1__0"), None);
        assert_eq!(lax_float(".5"), Some(0.5));
        assert_eq!(lax_float("5."), Some(5.0));
        assert_eq!(lax_float("0x10"), None);
        assert!(lax_float("Infinity").is_some_and(f64::is_infinite));
    }

    #[test]
    fn a_numeric_rounds_and_cuts_on_its_exact_text() {
        assert_eq!(round_numeric("2.5000000000000000").unwrap(), 2);
        assert_eq!(round_numeric("3.5").unwrap(), 4);
        assert_eq!(round_numeric("2.5000000000000001").unwrap(), 3);
        assert_eq!(round_numeric("-2.5").unwrap(), -2);
        assert_eq!(round_numeric("17").unwrap(), 17);
        assert_eq!(trunc_numeric("89.9999999999999999999999").unwrap(), 89);
        assert_eq!(trunc_numeric("-0.5").unwrap(), 0);
        assert!(trunc_numeric("NaN").is_err());
        assert_eq!(numeric_or_zero(Some("0".into())).unwrap(), PyNum::Int(0));
        assert_eq!(numeric_or_zero(Some("12".into())).unwrap(), PyNum::Int(12));
        assert_eq!(numeric_or_zero(None).unwrap(), PyNum::Int(0));
    }

    #[test]
    fn max_with_an_int_zero_first() {
        assert_eq!(max0(-1.5), PyNum::Int(0));
        assert_eq!(max0(0.0), PyNum::Int(0));
        assert_eq!(max0(2.5), PyNum::Float(2.5));
        assert_eq!(py_min(&[3.0, 1.0, 1.0, 2.0]), 1.0);
        assert_eq!(py_max(&[1.0, 3.0, 2.0]), 3.0);
    }
}
