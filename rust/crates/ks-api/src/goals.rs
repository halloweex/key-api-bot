//! The goal routes' seven GETs — `/api/goals`, `/history`, `/smart`,
//! `/seasonality`, `/growth`, `/weekly-patterns` and `/forecast` — answered
//! as `web/routes/api/goals.py` answers them under `KS_READ_GOALS=postgres`:
//! the statements `core/repositories/goals.py` renders for Postgres, each
//! read on the path `_goals_run` gives it (a stored goal raw, a forecast or a
//! shared goal table where its chain writes), and the Python arithmetic over
//! the rows, float by float — `round` half to even, `sum()` compensated, an
//! `int` 0 where Python never made a float, `min`/`max` keeping the first.
//!
//! An aggregate that divides — AVG, STDDEV, a quotient — is read as
//! `::float8`: Postgres gives such a numeric more digits than a `Decimal`
//! here holds, and `numeric_float8` rounds its exact text once, as
//! Python's `float(Decimal)` does — the same double either way.
//!
//! Two clocks, as in Python: the history behind a suggested goal asks the
//! database for Kyiv's today (`TODAY_IN_KYIV`), the calculators and the
//! smart goal take the request's.
//!
//! The parity is by contract: `api/client.ts` builds each query itself — a
//! sales type, the history's period type and weeks, a smart goal's month,
//! the forecast's month with `recalculate=false`. The integers are read as
//! pydantic reads them, as far as an `i64` reaches.

use std::collections::HashMap;

use chrono::{DateTime, Datelike, NaiveDate, TimeDelta, Utc};
use chrono_tz::Europe::Kyiv;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;
use serde::ser::{SerializeMap, Serializer};
use sqlx::Row;
use sqlx::postgres::PgRow;

use crate::gold::decimal_to_float;
use crate::params::{FieldError, PyInt, int_message, lax_bool, lax_int};
use crate::pgread::{Bind, PgReads};
use crate::pyrt::{PyNum, py_sum, round_float, round_num};
use crate::trend::Failure;
use crate::validate;

/// The surface `read_fallback` names in a 503.
const SURFACE: &str = "goals";
/// `TODAY_IN_KYIV`: what the database says today is, in Kyiv.
const TODAY: &str = "(now() AT TIME ZONE 'Europe/Kyiv')::date";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    Goals,
    History,
    Smart,
    Seasonality,
    Growth,
    Weekly,
    Forecast,
}

impl Route {
    /// The route for a contract endpoint name, if it is one of these.
    pub fn from_endpoint(name: &str) -> Option<Self> {
        Some(match name {
            "web.routes.api.goals.get_goals" => Self::Goals,
            "web.routes.api.goals.get_goal_history" => Self::History,
            "web.routes.api.goals.get_smart_goals" => Self::Smart,
            "web.routes.api.goals.get_seasonality_data" => Self::Seasonality,
            "web.routes.api.goals.get_growth_data" => Self::Growth,
            "web.routes.api.goals.get_weekly_patterns" => Self::Weekly,
            "web.routes.api.goals.get_goal_forecast" => Self::Forecast,
            _ => return None,
        })
    }
}

// ─── A map in Python's insertion order ──────────────────────────────────────

/// A `dict` as orjson writes it: in insertion order, an `int` key as its
/// digits (`OPT_NON_STR_KEYS`).
struct Ordered<K, V>(Vec<(K, V)>);

impl<K: ToString, V: Serialize> Serialize for Ordered<K, V> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(Some(self.0.len()))?;
        for (k, v) in &self.0 {
            map.serialize_entry(&k.to_string(), v)?;
        }
        map.end()
    }
}

impl<K: PartialEq, V> Ordered<K, V> {
    fn new() -> Self {
        Ordered(Vec::new())
    }

    /// `d[key] = value`: replaced where it stands, else appended.
    fn set(&mut self, key: K, value: V) {
        match self.0.iter_mut().find(|(k, _)| *k == key) {
            Some((_, slot)) => *slot = value,
            None => self.0.push((key, value)),
        }
    }

    /// `d.setdefault(key, value)`.
    fn setdefault(&mut self, key: K, value: V) -> &mut V {
        let at = match self.0.iter().position(|(k, _)| *k == key) {
            Some(at) => at,
            None => {
                self.0.push((key, value));
                self.0.len() - 1
            }
        };
        &mut self.0[at].1
    }
}

// ─── Python's numbers ───────────────────────────────────────────────────────

/// `round(x)` with no digits: the nearest integer, ties to even, an `int`.
pub fn round_int(x: f64) -> i64 {
    x.round_ties_even() as i64
}

/// `min(a, b)`: `b` only when strictly smaller.
fn py_min(a: f64, b: f64) -> f64 {
    if b < a { b } else { a }
}

/// `max(a, b)`: `b` only when strictly larger.
fn py_max(a: f64, b: f64) -> f64 {
    if b > a { b } else { a }
}

fn floats(values: impl IntoIterator<Item = f64>) -> Vec<PyNum> {
    values.into_iter().map(PyNum::Float).collect()
}

/// `_recency_weighted`: the oldest pair weighs 1.0, the newest 2.0.
pub fn recency_weighted(rates: &[f64]) -> f64 {
    let n = rates.len();
    if n == 1 {
        return rates[0];
    }
    let weights: Vec<f64> = (0..n).map(|i| 1.0 + (i as f64 / (n - 1) as f64)).collect();
    let products = floats(rates.iter().zip(&weights).map(|(r, w)| r * w));
    py_sum(&products).as_f64() / py_sum(&floats(weights)).as_f64()
}

// ─── The parameters ─────────────────────────────────────────────────────────

/// The route's parameters after FastAPI, before its own checks.
struct Query {
    sales_type: String,
    period_type: Option<String>,
    weeks_back: i64,
    year: Option<i64>,
    month: Option<i64>,
    recalculate: bool,
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
type Bound = (i64, &'static str);

/// An `int = Query(…, ge=lo, le=hi)`: pydantic's lax reading as far as an
/// `i64` reaches, then its bounds. pydantic's `ctx` is not carried: only
/// hostile 422s show it, and they are compared by status.
fn bounded(
    raw: String,
    name: &'static str,
    (lo, hi): (Bound, Bound),
    errors: &mut Vec<FieldError>,
) -> Option<i64> {
    let value = match lax_int(&raw) {
        Ok(PyInt::Small(v)) => v,
        Ok(PyInt::Big(_)) => {
            errors.push(field_error(
                "int_parsing",
                name,
                int_message("int_parsing"),
                Some(raw),
            ));
            return None;
        }
        Err(kind) => {
            errors.push(field_error(kind, name, int_message(kind), Some(raw)));
            return None;
        }
    };
    if value < lo.0 {
        errors.push(field_error("greater_than_equal", name, lo.1, Some(raw)));
    } else if value > hi.0 {
        errors.push(field_error("less_than_equal", name, hi.1, Some(raw)));
    }
    Some(value)
}

const YEAR: (Bound, Bound) = (
    (2020, "Input should be greater than or equal to 2020"),
    (2030, "Input should be less than or equal to 2030"),
);
const MONTH: (Bound, Bound) = (
    (1, "Input should be greater than or equal to 1"),
    (12, "Input should be less than or equal to 12"),
);
const WEEKS: (Bound, Bound) = MONTH;

fn missing(name: &'static str, errors: &mut Vec<FieldError>) {
    errors.push(field_error("missing", name, "Field required", None));
}

fn parse(route: Route, raw: &str) -> Result<Query, Failure> {
    let mut last = HashMap::new();
    for (name, value) in form_urlencoded::parse(raw.as_bytes()) {
        last.insert(name.into_owned(), value.into_owned());
    }
    let mut take = |name: &str| last.remove(name);
    let mut errors = Vec::new();
    let mut q = Query {
        sales_type: String::new(),
        period_type: None,
        weeks_back: 4,
        year: None,
        month: None,
        recalculate: false,
    };
    // FastAPI reads each signature in its order.
    match route {
        Route::History => {
            q.period_type = take("period_type");
            if q.period_type.is_none() {
                missing("period_type", &mut errors);
            }
            if let Some(raw) = take("weeks_back") {
                q.weeks_back = bounded(raw, "weeks_back", WEEKS, &mut errors).unwrap_or(4);
            }
        }
        Route::Smart => {
            q.sales_type = take("sales_type").unwrap_or_else(|| "retail".to_owned());
            q.year = take("year").and_then(|raw| bounded(raw, "year", YEAR, &mut errors));
            q.month = take("month").and_then(|raw| bounded(raw, "month", MONTH, &mut errors));
        }
        Route::Forecast => {
            match take("year") {
                Some(raw) => q.year = bounded(raw, "year", YEAR, &mut errors),
                None => missing("year", &mut errors),
            }
            match take("month") {
                Some(raw) => q.month = bounded(raw, "month", MONTH, &mut errors),
                None => missing("month", &mut errors),
            }
        }
        _ => {}
    }
    if route != Route::Smart {
        q.sales_type = take("sales_type").unwrap_or_else(|| "retail".to_owned());
    }
    if route == Route::Forecast
        && let Some(raw) = take("recalculate")
    {
        match lax_bool(&raw) {
            Some(v) => q.recalculate = v,
            None => errors.push(field_error(
                "bool_parsing",
                "recalculate",
                "Input should be a valid boolean, unable to interpret input",
                Some(raw),
            )),
        }
    }
    if !errors.is_empty() {
        return Err(Failure::Unprocessable(errors));
    }
    Ok(q)
}

// ─── Reading a row ──────────────────────────────────────────────────────────

/// A numeric column as `float()` reads it, or None for a NULL.
fn num(row: &PgRow, i: usize) -> Result<Option<f64>, Failure> {
    if let Ok(v) = row.try_get::<Option<Decimal>, _>(i) {
        return Ok(v.map(decimal_to_float));
    }
    if let Ok(v) = row.try_get::<Option<f64>, _>(i) {
        return Ok(v);
    }
    if let Ok(v) = row.try_get::<Option<i64>, _>(i) {
        return Ok(v.map(|n| n as f64));
    }
    row.try_get::<Option<i32>, _>(i)
        .map(|v| v.map(f64::from))
        .map_err(|_| Failure::Raised("a column the statement sums is not a number"))
}

/// `float(x or default)`: a NULL or a zero is the default.
fn num_or(row: &PgRow, i: usize, default: f64) -> Result<f64, Failure> {
    Ok(num(row, i)?.filter(|v| *v != 0.0).unwrap_or(default))
}

/// `int(x)` of an integer or a numeric column (`EXTRACT` is a numeric).
fn int_col(row: &PgRow, i: usize) -> Result<Option<i64>, Failure> {
    if let Ok(v) = row.try_get::<Option<i64>, _>(i) {
        return Ok(v);
    }
    if let Ok(v) = row.try_get::<Option<i32>, _>(i) {
        return Ok(v.map(i64::from));
    }
    let v: Option<Decimal> = row
        .try_get(i)
        .map_err(|_| Failure::Raised("a column the statement counts is not a number"))?;
    v.map(|d| {
        d.trunc()
            .to_i64()
            .ok_or(Failure::Raised("an integer past 64 bits"))
    })
    .transpose()
}

fn int_req(row: &PgRow, i: usize) -> Result<i64, Failure> {
    int_col(row, i)?.ok_or(Failure::Raised("int(None)"))
}

fn text(row: &PgRow, i: usize) -> Result<Option<String>, Failure> {
    row.try_get(i)
        .map_err(|_| Failure::Raised("a column the statement names is not text"))
}

/// `float(value or 0) if value else 0`: the `int` 0 for a NULL or a zero.
fn float_or_int_zero(value: Option<f64>) -> PyNum {
    match value {
        Some(v) if v != 0.0 => PyNum::Float(v),
        _ => PyNum::Int(0),
    }
}

// ─── The reads ──────────────────────────────────────────────────────────────

/// A read through `_goals_run`: a failure is a refusal naming `goals` —
/// `fall_back` under `off`, or `chain_refusal` for a chain's table.
async fn read(store: &PgReads, sql: String, binds: &[Bind]) -> Result<Vec<PgRow>, Failure> {
    store
        .rows(sql, binds)
        .await
        .map_err(|e| Failure::Unavailable(SURFACE, e))
}

/// One stored goal: its period, its amount, whether a person set it.
type StoredGoal = (String, f64, Option<bool>);

/// The stored goals, read where chain 7a writes, with no fallback: in Python
/// a failure there is a raw exception, the app's 500.
async fn stored_goals(store: &PgReads) -> Result<Vec<StoredGoal>, Failure> {
    let rows = store
        .rows(
            "
    SELECT period_type, goal_amount, is_custom, calculated_goal, growth_factor
    FROM app.revenue_goals
    ORDER BY period_type
"
            .to_owned(),
            &[],
        )
        .await
        .map_err(|_| Failure::Raised("the stored goals could not be read"))?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let period: Option<String> = text(row, 0)?;
        let amount = num(row, 1)?.ok_or(Failure::Raised("float(None)"))?;
        let custom: Option<bool> = row
            .try_get(2)
            .map_err(|_| Failure::Raised("is_custom is not a boolean"))?;
        out.push((period.unwrap_or_default(), amount, custom));
    }
    Ok(out)
}

/// `_silver_history_where`: not a return, and one sales type unless `all`.
/// No `is_active_source`: a growth baseline counts every source.
fn history_where(sales_type: &str, binds: &mut Vec<Bind>) -> String {
    if sales_type == "all" {
        return "NOT s.is_return".to_owned();
    }
    binds.push(Bind::Text(sales_type.to_owned()));
    format!("NOT s.is_return AND s.sales_type = ${}", binds.len())
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct History {
    period_type: String,
    average: f64,
    min: f64,
    max: f64,
    period_count: i64,
    std_dev: f64,
    trend: f64,
    weeks_analyzed: i64,
}

/// `get_historical_revenue`: the window ends at the database's Kyiv today.
async fn history(
    store: &PgReads,
    period_type: &str,
    weeks_back: i64,
    sales_type: &str,
) -> Result<History, Failure> {
    let mut binds = Vec::new();
    let mut clauses = vec![
        "NOT s.is_return".to_owned(),
        "s.is_active_source".to_owned(),
    ];
    if sales_type != "all" {
        binds.push(Bind::Text(sales_type.to_owned()));
        clauses.push("s.sales_type = $1".to_owned());
    }
    let (grain, floor, ceiling) = match period_type {
        "daily" => (
            "s.order_date".to_owned(),
            format!("{TODAY} - INTERVAL '{} days'", weeks_back * 7),
            TODAY.to_owned(),
        ),
        "weekly" => (
            "DATE_TRUNC('week', s.order_date)".to_owned(),
            format!("{TODAY} - INTERVAL '{} days'", weeks_back * 7),
            format!("DATE_TRUNC('week', {TODAY})"),
        ),
        _ => (
            "DATE_TRUNC('month', s.order_date)".to_owned(),
            format!("{TODAY} - INTERVAL '{} months'", (weeks_back / 4).max(3)),
            format!("DATE_TRUNC('month', {TODAY})"),
        ),
    };
    let mut wh = vec![
        format!("s.order_date >= {floor}"),
        format!("s.order_date < {ceiling}"),
    ];
    wh.extend(clauses);
    let wh = wh.join(" AND ");
    let rows = read(
        store,
        format!(
            "
    WITH period_revenue AS (
        SELECT {grain} AS period_start,
               SUM(s.grand_total) AS revenue
        FROM silver.orders s
        WHERE {wh}
        GROUP BY {grain}
    )
    SELECT AVG(revenue)::float8, MIN(revenue), MAX(revenue), COUNT(*), STDDEV(revenue)::float8
    FROM period_revenue
"
        ),
        &binds,
    )
    .await?;
    let row = rows.first();
    let pick = |i: usize| -> Result<f64, Failure> {
        Ok(match row {
            Some(r) => num(r, i)?.unwrap_or(0.0),
            None => 0.0,
        })
    };
    let (avg, min, max, std) = (pick(0)?, pick(1)?, pick(2)?, pick(4)?);
    let count = match row {
        Some(r) => int_col(r, 3)?.unwrap_or(0),
        None => 0,
    };
    let mut trend = 0.0;
    if period_type == "weekly" && count >= 4 {
        let rows = read(
            store,
            format!(
                "
    WITH weekly_revenue AS (
        SELECT DATE_TRUNC('week', s.order_date) AS week_start,
               SUM(s.grand_total) AS revenue,
               ROW_NUMBER() OVER (
                   ORDER BY DATE_TRUNC('week', s.order_date) DESC) AS week_num
        FROM silver.orders s
        WHERE {wh}
        GROUP BY DATE_TRUNC('week', s.order_date)
    )
    SELECT AVG(CASE WHEN week_num <= 2 THEN revenue END)::float8,
           AVG(CASE WHEN week_num > 2 THEN revenue END)::float8
    FROM weekly_revenue
"
            ),
            &binds,
        )
        .await?;
        let (recent, older) = match rows.first() {
            Some(r) => (num(r, 0)?.unwrap_or(0.0), num(r, 1)?.unwrap_or(0.0)),
            None => (0.0, 0.0),
        };
        if older > 0.0 {
            trend = ((recent - older) / older) * 100.0;
        }
    }
    Ok(History {
        period_type: period_type.to_owned(),
        average: round_float(avg, 2),
        min: round_float(min, 2),
        max: round_float(max, 2),
        period_count: count,
        std_dev: round_float(std, 2),
        trend: round_float(trend, 1),
        weeks_analyzed: weeks_back,
    })
}

fn confidence_of(count: i64) -> &'static str {
    if count >= 4 {
        "high"
    } else if count >= 2 {
        "medium"
    } else {
        "low"
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Goal {
    amount: PyNum,
    is_custom: Option<bool>,
    suggested_amount: i64,
    based_on_average: f64,
    trend: f64,
    confidence: &'static str,
}

/// `get_goals`: the stored goals first, then a suggestion per period from
/// four weeks of history times 1.10.
async fn goals(store: &PgReads, sales_type: &str) -> Result<Ordered<&'static str, Goal>, Failure> {
    let stored = stored_goals(store).await?;
    let mut out = Ordered::new();
    for (period, step, floor) in [
        ("daily", 10_000, 50_000),
        ("weekly", 50_000, 300_000),
        ("monthly", 100_000, 1_000_000),
    ] {
        let h = history(store, period, 4, sales_type).await?;
        let suggested = (round_int(h.average * 1.10 / step as f64) * step).max(floor);
        // The last row of a period wins, as a dict comprehension keeps it.
        let found = stored.iter().rev().find(|(p, _, _)| p == period);
        out.set(
            period,
            Goal {
                amount: found.map_or(PyNum::Int(suggested), |(_, a, _)| PyNum::Float(*a)),
                is_custom: found.map_or(Some(false), |(_, _, c)| *c),
                suggested_amount: suggested,
                based_on_average: h.average,
                trend: h.trend,
                confidence: confidence_of(h.period_count),
            },
        );
    }
    Ok(out)
}

#[derive(Serialize)]
struct Seasonal {
    month: i64,
    avg_revenue: f64,
    min_revenue: f64,
    max_revenue: f64,
    sample_size: i64,
    std_dev: f64,
    seasonality_index: f64,
    confidence: &'static str,
}

/// `calculate_seasonality_indices`.
async fn seasonality(store: &PgReads, sales_type: &str) -> Result<Ordered<i64, Seasonal>, Failure> {
    let mut binds = Vec::new();
    let wh = history_where(sales_type, &mut binds);
    let rows = read(
        store,
        format!(
            "
    WITH monthly_data AS (
        SELECT EXTRACT(YEAR FROM s.order_date) AS year,
               EXTRACT(MONTH FROM s.order_date) AS month,
               SUM(s.grand_total) AS revenue,
               COUNT(DISTINCT s.order_date) AS days_with_orders
        FROM silver.orders s
        WHERE {wh}
        GROUP BY EXTRACT(YEAR FROM s.order_date), EXTRACT(MONTH FROM s.order_date)
        HAVING COUNT(DISTINCT s.order_date) >= 20
    ),
    monthly_stats AS (
        SELECT month,
               AVG(revenue) AS avg_revenue,
               MIN(revenue) AS min_revenue,
               MAX(revenue) AS max_revenue,
               COUNT(*) AS sample_size,
               STDDEV(revenue) AS std_dev
        FROM monthly_data
        GROUP BY month
    ),
    overall_avg AS (
        SELECT AVG(avg_revenue) AS grand_avg FROM monthly_stats
    )
    SELECT ms.month, ms.avg_revenue::float8, ms.min_revenue, ms.max_revenue,
           ms.sample_size, ms.std_dev::float8, (ms.avg_revenue / oa.grand_avg)::float8
    FROM monthly_stats ms, overall_avg oa
    ORDER BY ms.month
"
        ),
        &binds,
    )
    .await?;
    let mut out = Ordered::new();
    for row in &rows {
        let month = int_req(row, 0)?;
        let sample_size = int_col(row, 4)?.unwrap_or(0);
        out.set(
            month,
            Seasonal {
                month,
                avg_revenue: round_float(num_or(row, 1, 0.0)?, 2),
                min_revenue: round_float(num_or(row, 2, 0.0)?, 2),
                max_revenue: round_float(num_or(row, 3, 0.0)?, 2),
                sample_size,
                std_dev: round_float(num_or(row, 5, 0.0)?, 2),
                seasonality_index: round_float(num_or(row, 6, 1.0)?, 4),
                confidence: match sample_size {
                    3.. => "high",
                    2 => "medium",
                    _ => "low",
                },
            },
        );
    }
    Ok(out)
}

#[derive(Serialize)]
struct YearRow {
    year: i64,
    revenue: f64,
}

#[derive(Serialize)]
struct Growth {
    overall_yoy: f64,
    monthly_yoy: Ordered<i64, f64>,
    yearly_data: Vec<YearRow>,
    sample_size: usize,
}

/// `calculate_yoy_growth`: full years before this Kyiv year, and the same
/// month a year apart.
async fn growth(store: &PgReads, sales_type: &str, today: NaiveDate) -> Result<Growth, Failure> {
    let this_year = NaiveDate::from_ymd_opt(today.year(), 1, 1).expect("a first of January");
    let mut binds = Vec::new();
    let wh = history_where(sales_type, &mut binds);
    let mut yearly_binds = binds.clone();
    yearly_binds.push(Bind::Date(this_year));
    let yearly = read(
        store,
        format!(
            "
    WITH yearly_data AS (
        SELECT EXTRACT(YEAR FROM s.order_date) AS year,
               SUM(s.grand_total) AS revenue,
               COUNT(DISTINCT EXTRACT(MONTH FROM s.order_date)) AS months_active
        FROM silver.orders s
        WHERE {wh}
          AND s.order_date < ${}
        GROUP BY EXTRACT(YEAR FROM s.order_date)
    )
    SELECT year, revenue FROM yearly_data
    WHERE months_active >= 11
    ORDER BY year
",
            yearly_binds.len()
        ),
        &yearly_binds,
    )
    .await?;
    let monthly = read(
        store,
        format!(
            "
    WITH monthly_by_year AS (
        SELECT EXTRACT(YEAR FROM s.order_date) AS year,
               EXTRACT(MONTH FROM s.order_date) AS month,
               SUM(s.grand_total) AS revenue
        FROM silver.orders s
        WHERE {wh}
        GROUP BY EXTRACT(YEAR FROM s.order_date), EXTRACT(MONTH FROM s.order_date)
        HAVING COUNT(DISTINCT s.order_date) >= 25
    )
    SELECT curr.month, curr.year,
           ((curr.revenue - prev.revenue) / NULLIF(prev.revenue, 0))::float8
    FROM monthly_by_year curr
    JOIN monthly_by_year prev
      ON curr.month = prev.month AND curr.year = prev.year + 1
    ORDER BY curr.month, curr.year
"
        ),
        &binds,
    )
    .await?;

    let mut years = Vec::with_capacity(yearly.len());
    for row in &yearly {
        years.push((
            int_req(row, 0)?,
            num(row, 1)?.ok_or(Failure::Raised("float(None)"))?,
        ));
    }
    let mut rates = Vec::new();
    for pair in years.windows(2) {
        let (prev, curr) = (pair[0].1, pair[1].1);
        if prev > 0.0 {
            rates.push((curr - prev) / prev);
        }
    }
    let overall = if rates.is_empty() {
        0.10
    } else {
        recency_weighted(&rates)
    };
    let mut pairs: Ordered<i64, Vec<f64>> = Ordered::new();
    for row in &monthly {
        if let Some(yoy) = num(row, 2)? {
            pairs.setdefault(int_req(row, 0)?, Vec::new()).push(yoy);
        }
    }
    Ok(Growth {
        overall_yoy: round_float(overall, 4),
        monthly_yoy: Ordered(
            pairs
                .0
                .iter()
                .map(|(m, r)| (*m, round_float(recency_weighted(r), 4)))
                .collect(),
        ),
        yearly_data: years
            .iter()
            .map(|(year, revenue)| YearRow {
                year: *year,
                revenue: round_float(*revenue, 2),
            })
            .collect(),
        sample_size: rates.len(),
    })
}

/// `_DEFAULT_WEEKLY_WEIGHTS`.
const DEFAULT_WEIGHTS: [(i64, f64); 5] = [(1, 0.23), (2, 0.23), (3, 0.23), (4, 0.23), (5, 0.08)];

/// A weekly pattern per month, each week its weight.
type Patterns = Ordered<i64, Ordered<i64, f64>>;

/// `calculate_weekly_patterns`: every month and week filled for the reader.
async fn weekly(store: &PgReads, sales_type: &str) -> Result<Patterns, Failure> {
    let mut binds = Vec::new();
    let wh = history_where(sales_type, &mut binds);
    let rows = read(
        store,
        format!(
            "
    WITH weekly_data AS (
        SELECT EXTRACT(YEAR FROM s.order_date) AS year,
               EXTRACT(MONTH FROM s.order_date) AS month,
               LEAST(5, CAST(CEIL(EXTRACT(DAY FROM s.order_date) / 7.0) AS INTEGER))
                   AS week_of_month,
               SUM(s.grand_total) AS revenue
        FROM silver.orders s
        WHERE {wh}
        GROUP BY EXTRACT(YEAR FROM s.order_date),
                 EXTRACT(MONTH FROM s.order_date),
                 LEAST(5, CAST(CEIL(EXTRACT(DAY FROM s.order_date) / 7.0) AS INTEGER))
    ),
    monthly_totals AS (
        SELECT year, month, SUM(revenue) AS month_total
        FROM weekly_data
        GROUP BY year, month
    ),
    weekly_weights AS (
        SELECT wd.month, wd.week_of_month,
               AVG(wd.revenue / NULLIF(mt.month_total, 0)) AS avg_weight,
               COUNT(*) AS sample_size
        FROM weekly_data wd
        JOIN monthly_totals mt ON wd.year = mt.year AND wd.month = mt.month
        GROUP BY wd.month, wd.week_of_month
    )
    SELECT month, week_of_month, avg_weight::float8, sample_size
    FROM weekly_weights
    ORDER BY month, week_of_month
"
        ),
        &binds,
    )
    .await?;
    let mut patterns: Patterns = Ordered::new();
    for row in &rows {
        let month = int_req(row, 0)?;
        let week = int_req(row, 1)?;
        let weight = num_or(row, 2, 0.25)?;
        // `int(row[3])`: the sample size, which a NULL would make a 500.
        int_req(row, 3)?;
        patterns
            .setdefault(month, Ordered::new())
            .set(week, round_float(weight, 4));
    }
    for month in 1..=12 {
        let weeks = patterns.setdefault(month, Ordered::new());
        for (week, weight) in DEFAULT_WEIGHTS {
            weeks.setdefault(week, weight);
        }
    }
    Ok(patterns)
}

fn first_of_month(year: i32, month: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(year, month, 1).expect("a validated month")
}

/// `calendar.monthrange(year, month)[1]`.
fn days_in_month(year: i32, month: u32) -> u32 {
    let next = if month == 12 {
        first_of_month(year + 1, 1)
    } else {
        first_of_month(year, month + 1)
    };
    next.pred_opt().map_or(31, |d| d.day())
}

/// `_get_ml_forecast_total`: what the month has earned before today, from
/// Gold, plus what the model expects of the rest — 0 with no forecast.
async fn ml_total(
    store: &PgReads,
    year: i32,
    month: u32,
    sales_type: &str,
    today: NaiveDate,
) -> Result<f64, Failure> {
    let first = first_of_month(year, month);
    let last = first
        .with_day(days_in_month(year, month))
        .expect("the last day of the month");
    let mut actual = 0.0;
    if first < today {
        let end = (today - TimeDelta::days(1)).min(last);
        let (filter, binds) = if sales_type == "retail" || sales_type == "b2b" {
            (
                "sales_type = $3",
                vec![
                    Bind::Date(first),
                    Bind::Date(end),
                    Bind::Text(sales_type.to_owned()),
                ],
            )
        } else {
            ("1=1", vec![Bind::Date(first), Bind::Date(end)])
        };
        let rows = read(
            store,
            format!(
                "
    SELECT COALESCE(SUM(revenue), 0)
    FROM gold.daily_revenue
    WHERE date BETWEEN $1 AND $2
      AND source_id IS NULL
      AND {filter}
"
            ),
            &binds,
        )
        .await?;
        actual = match rows.first() {
            Some(r) => num(r, 0)?.unwrap_or(0.0),
            None => 0.0,
        };
    }
    let start = first.max(today);
    let rows = read(
        store,
        "
    SELECT SUM(predicted_revenue)
    FROM app.revenue_predictions
    WHERE sales_type = $1
      AND prediction_date >= $2
      AND prediction_date <= $3
"
        .to_owned(),
        &[
            Bind::Text(sales_type.to_owned()),
            Bind::Date(start),
            Bind::Date(last),
        ],
    )
    .await?;
    let predicted = match rows.first() {
        Some(r) => num(r, 0)?.filter(|v| *v != 0.0).unwrap_or(0.0),
        None => 0.0,
    };
    if predicted == 0.0 {
        return Ok(0.0);
    }
    Ok(actual + predicted)
}

/// `_dynamic_growth_cap`: `max(0.10, min(0.50, avg + 1.5 × stddev))` over
/// the month's consecutive-year YoY, 0.35 without a pair.
async fn growth_cap(store: &PgReads, month: u32, sales_type: &str) -> Result<f64, Failure> {
    let mut binds = vec![Bind::Int(i64::from(month))];
    let wh = history_where(sales_type, &mut binds);
    let rows = read(
        store,
        format!(
            "
    WITH monthly_by_year AS (
        SELECT EXTRACT(YEAR FROM s.order_date) AS year,
               SUM(s.grand_total) AS revenue
        FROM silver.orders s
        WHERE EXTRACT(MONTH FROM s.order_date) = $1
          AND {wh}
        GROUP BY EXTRACT(YEAR FROM s.order_date)
        HAVING COUNT(DISTINCT s.order_date) >= 25
    ),
    yoy_pairs AS (
        SELECT (curr.revenue - prev.revenue) / NULLIF(prev.revenue, 0) AS yoy
        FROM monthly_by_year curr
        JOIN monthly_by_year prev ON curr.year = prev.year + 1
    )
    SELECT AVG(yoy)::float8, STDDEV(yoy)::float8, COUNT(*) FROM yoy_pairs
"
        ),
        &binds,
    )
    .await?;
    let Some(row) = rows.first() else {
        return Ok(0.35);
    };
    Ok(growth_cap_of(
        num(row, 0)?,
        num(row, 1)?,
        int_col(row, 2)?.unwrap_or(0),
    ))
}

/// `_growth_cap((avg, stddev, count))`: 0.35 without a pair, else
/// `max(0.10, min(0.50, avg + 1.5 × stddev))`.
pub fn growth_cap_of(avg: Option<f64>, std: Option<f64>, count: i64) -> f64 {
    match avg {
        Some(avg) if count >= 1 => py_max(0.10, py_min(0.50, avg + 1.5 * std.unwrap_or(0.0))),
        _ => 0.35,
    }
}

/// `_last_year_month_revenue`: the target month a year earlier.
async fn last_year(
    store: &PgReads,
    year: i32,
    month: u32,
    sales_type: &str,
) -> Result<PyNum, Failure> {
    let first = first_of_month(year - 1, month);
    let next = if month == 12 {
        first_of_month(year, 1)
    } else {
        first_of_month(year - 1, month + 1)
    };
    let mut binds = vec![Bind::Date(first), Bind::Date(next)];
    let wh = history_where(sales_type, &mut binds);
    let rows = read(
        store,
        format!(
            "
    SELECT SUM(s.grand_total)
    FROM silver.orders s
    WHERE s.order_date >= $1 AND s.order_date < $2
      AND {wh}
"
        ),
        &binds,
    )
    .await?;
    Ok(float_or_int_zero(match rows.first() {
        Some(r) => num(r, 0)?,
        None => None,
    }))
}

/// `_recent_three_month_average`: the last three complete months, by the
/// first of this Kyiv month.
async fn recent_three(
    store: &PgReads,
    sales_type: &str,
    today: NaiveDate,
) -> Result<PyNum, Failure> {
    let month_start = today.with_day(1).expect("every month has a first day");
    let mut binds = Vec::new();
    let wh = history_where(sales_type, &mut binds);
    binds.push(Bind::Date(month_start));
    let rows = read(
        store,
        format!(
            "
    WITH monthly_revenue AS (
        SELECT EXTRACT(YEAR FROM s.order_date) AS year,
               EXTRACT(MONTH FROM s.order_date) AS month,
               SUM(s.grand_total) AS revenue
        FROM silver.orders s
        WHERE {wh}
          AND s.order_date < ${}
        GROUP BY EXTRACT(YEAR FROM s.order_date), EXTRACT(MONTH FROM s.order_date)
        HAVING COUNT(DISTINCT s.order_date) >= 25
        ORDER BY year DESC, month DESC
        LIMIT 3
    )
    SELECT AVG(revenue)::float8 FROM monthly_revenue
",
            binds.len()
        ),
        &binds,
    )
    .await?;
    Ok(float_or_int_zero(match rows.first() {
        Some(r) => num(r, 0)?,
        None => None,
    }))
}

/// One month's seasonal row: the index, the average, the YoY, the confidence.
pub type SeasonalRow = (Option<f64>, Option<f64>, Option<f64>, Option<String>);

/// What a smart goal reads of the three shared goal tables, in one
/// statement, where chain 7b-3 writes them.
struct Shared {
    seasonal: Option<SeasonalRow>,
    growth: Option<Option<f64>>,
    weekly: Vec<(i64, f64)>,
}

async fn shared(store: &PgReads, month: u32) -> Result<Shared, Failure> {
    let m = i64::from(month);
    let rows = read(
        store,
        "
    SELECT 'seasonal' AS part, CAST(NULL AS INTEGER) AS week,
           seasonality_index AS a, avg_revenue AS b, yoy_growth AS c,
           confidence AS d
    FROM app.seasonal_indices
    WHERE month = $1
    UNION ALL
    SELECT 'growth', NULL, value, NULL, NULL, NULL
    FROM app.growth_metrics
    WHERE metric_type = 'yoy_overall'
    UNION ALL
    SELECT 'weekly', week_of_month, weight, NULL, NULL, NULL
    FROM app.weekly_patterns
    WHERE month = $2
"
        .to_owned(),
        &[Bind::Int(m), Bind::Int(m)],
    )
    .await?;
    let mut out = Shared {
        seasonal: None,
        growth: None,
        weekly: Vec::new(),
    };
    for row in &rows {
        match text(row, 0)?.as_deref() {
            Some("seasonal") if out.seasonal.is_none() => {
                out.seasonal = Some((num(row, 2)?, num(row, 3)?, num(row, 4)?, text(row, 5)?));
            }
            Some("growth") if out.growth.is_none() => out.growth = Some(num(row, 2)?),
            Some("weekly") => {
                // `float(row[1])` of a NULL weight is Python's TypeError.
                let weight = num(row, 2)?.ok_or(Failure::Raised("float(None)"))?;
                out.weekly.push((int_req(row, 1)?, weight));
            }
            _ => {}
        }
    }
    // `sorted(…, key=week)`: stable, and the weeks are a key anyway.
    out.weekly.sort_by_key(|(week, _)| *week);
    Ok(out)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Monthly {
    goal: i64,
    last_year_revenue: PyNum,
    #[serde(rename = "recent3MonthAvg")]
    recent_3_month_avg: PyNum,
    historical_avg: PyNum,
    yoy_goal: f64,
    recent_goal: f64,
    ml_forecast_goal: f64,
    growth_rate: f64,
    growth_cap: f64,
    seasonality_index: f64,
    confidence: String,
    calculation_method: &'static str,
    blend_weights: Option<Ordered<&'static str, f64>>,
}

#[derive(Serialize)]
struct Weekly {
    goal: i64,
    breakdown: Ordered<i64, i64>,
    weights: Ordered<i64, f64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Daily {
    goal: i64,
    days_in_month: u32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Metadata {
    #[serde(rename = "overallYoY")]
    overall_yoy: f64,
    #[serde(rename = "monthlyYoY")]
    monthly_yoy: f64,
    calculated_at: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Smart {
    target_year: i64,
    target_month: i64,
    monthly: Monthly,
    weekly: Weekly,
    daily: Daily,
    metadata: Metadata,
}

/// `datetime.now(DEFAULT_TZ).isoformat()`: Kyiv's wall clock, microseconds
/// only when there are any, and the offset.
fn kyiv_isoformat(now: DateTime<Utc>) -> String {
    let local = now.with_timezone(&Kyiv);
    let base = local.format("%Y-%m-%dT%H:%M:%S");
    let offset = local.format("%:z");
    match local.timestamp_subsec_micros() {
        0 => format!("{base}{offset}"),
        micros => format!("{base}.{micros:06}{offset}"),
    }
}

/// `generate_smart_goals`: three signals — last year's month grown, the
/// recent months seasonally adjusted, the model's forecast — blended by the
/// month's confidence, then a weekly and a daily split.
async fn smart(
    store: &PgReads,
    year: i64,
    month: i64,
    sales_type: &str,
    now: DateTime<Utc>,
) -> Result<Smart, Failure> {
    let today = now.with_timezone(&Kyiv).date_naive();
    let (y, m) = (
        i32::try_from(year).map_err(|_| Failure::Raised("a year past i32"))?,
        u32::try_from(month).map_err(|_| Failure::Raised("a month past u32"))?,
    );
    let ml = ml_total(store, y, m, sales_type, today).await?;
    let cap = growth_cap(store, m, sales_type).await?;
    let last_year_revenue = last_year(store, y, m, sales_type).await?;
    let recent = recent_three(store, sales_type, today).await?;
    let tables = shared(store, m).await?;
    Ok(compose(
        Inputs {
            year,
            month,
            ml,
            cap,
            last_year: last_year_revenue,
            recent,
            seasonal: tables.seasonal,
            growth: tables.growth,
            weekly: tables.weekly,
        },
        now,
    ))
}

/// What a smart goal is computed from, once read.
pub struct Inputs {
    pub year: i64,
    pub month: i64,
    /// `_get_ml_forecast_total`.
    pub ml: f64,
    /// `_dynamic_growth_cap`.
    pub cap: f64,
    /// `_last_year_month_revenue`: the int 0, or a float.
    pub last_year: PyNum,
    /// `_recent_three_month_average`: the int 0, or a float.
    pub recent: PyNum,
    /// The month's row of `seasonal_indices`, if there is one.
    pub seasonal: Option<SeasonalRow>,
    /// `growth_metrics`' overall YoY: no row, a NULL, or a value.
    pub growth: Option<Option<f64>>,
    /// The month's weekly weights, sorted by week.
    pub weekly: Vec<(i64, f64)>,
}

/// `generate_smart_goals` as the bytes orjson writes, from what it read.
pub fn compose_json(inputs: Inputs, now: DateTime<Utc>) -> Vec<u8> {
    json(&compose(inputs, now))
}

/// The arithmetic of `generate_smart_goals`, after its reads.
fn compose(i: Inputs, now: DateTime<Utc>) -> Smart {
    let (year, month, ml, cap) = (i.year, i.month, i.ml, i.cap);
    let (y, m) = (
        i32::try_from(year).expect("a validated year"),
        u32::try_from(month).expect("a validated month"),
    );
    let (last_year_revenue, recent) = (i.last_year, i.recent);
    let tables = Shared {
        seasonal: i.seasonal,
        growth: i.growth,
        weekly: i.weekly,
    };

    let (index, historical_avg, monthly_yoy, confidence) = match &tables.seasonal {
        Some((a, b, c, d)) => (
            a.filter(|v| *v != 0.0).unwrap_or(1.0),
            PyNum::Float(b.filter(|v| *v != 0.0).unwrap_or(0.0)),
            c.filter(|v| *v != 0.0).unwrap_or(cap),
            d.clone()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "low".to_owned()),
        ),
        None => (1.0, PyNum::Int(0), cap, "low".to_owned()),
    };
    let overall_yoy = match tables.growth {
        Some(value) => value.filter(|v| *v != 0.0).unwrap_or(cap),
        None => cap,
    };
    let raw_growth = if monthly_yoy > 0.0 {
        monthly_yoy
    } else {
        overall_yoy
    };
    let mut growth_rate = py_min(raw_growth, cap);

    let mut yoy_goal = 0.0;
    if last_year_revenue.as_f64() > 0.0 {
        yoy_goal = last_year_revenue.as_f64() * (1.0 + growth_rate);
    }
    let mut recent_goal = 0.0;
    if recent.as_f64() > 0.0 && index > 0.0 {
        recent_goal = recent.as_f64() * index;
    }

    let base: [(&'static str, f64); 3] = match confidence.as_str() {
        "high" => [("yoy", 0.40), ("recent", 0.30), ("ml", 0.30)],
        "medium" => [("yoy", 0.30), ("recent", 0.35), ("ml", 0.35)],
        _ => [("yoy", 0.20), ("recent", 0.40), ("ml", 0.40)],
    };
    let weight_of = |k: &str| base.iter().find(|(n, _)| *n == k).map_or(0.0, |(_, w)| *w);
    let signals: Vec<(&'static str, f64)> =
        [("yoy", yoy_goal), ("recent", recent_goal), ("ml", ml)]
            .into_iter()
            .filter(|(_, v)| *v > 0.0)
            .collect();

    let (monthly_value, method, blend) = if !signals.is_empty() {
        let available = py_sum(&floats(signals.iter().map(|(k, _)| weight_of(k)))).as_f64();
        let blend: Vec<(&'static str, f64)> = signals
            .iter()
            .map(|(k, _)| (*k, weight_of(k) / available))
            .collect();
        let goal = py_sum(&floats(
            signals.iter().zip(&blend).map(|((_, s), (_, w))| s * w),
        ));
        // `max(blend_weights, key=…)`: the first of the largest.
        let mut dominant = 0;
        for i in 1..signals.len() {
            if blend[i].1 * signals[i].1 > blend[dominant].1 * signals[dominant].1 {
                dominant = i;
            }
        }
        let method = match blend[dominant].0 {
            "yoy" => "yoy_growth",
            "recent" => "recent_trend",
            _ => "ml_forecast",
        };
        (goal, method, blend)
    } else if historical_avg.as_f64() > 0.0 {
        (
            PyNum::Float(historical_avg.as_f64() * (1.0 + growth_rate)),
            "historical_avg",
            Vec::new(),
        )
    } else {
        growth_rate = cap;
        (PyNum::Int(3_000_000), "fallback", Vec::new())
    };
    let monthly_goal = round_int(monthly_value.as_f64() / 100_000.0) * 100_000;

    let mut weights: Vec<(i64, f64)> = if tables.weekly.is_empty() {
        DEFAULT_WEIGHTS.to_vec()
    } else {
        tables.weekly.clone()
    };
    let total = py_sum(&floats(weights.iter().map(|(_, w)| *w))).as_f64();
    if total > 0.0 {
        for (_, w) in &mut weights {
            *w /= total;
        }
    }
    let mut breakdown: Vec<(i64, i64)> = weights
        .iter()
        .map(|(week, w)| {
            (
                *week,
                round_int(monthly_goal as f64 * w / 10_000.0) * 10_000,
            )
        })
        .collect();
    let residual = monthly_goal - breakdown.iter().map(|(_, g)| g).sum::<i64>();
    if residual != 0 && !breakdown.is_empty() {
        let mut largest = 0;
        for i in 1..breakdown.len() {
            if breakdown[i].1 > breakdown[largest].1 {
                largest = i;
            }
        }
        breakdown[largest].1 += residual;
    }
    let days = days_in_month(y, m);
    let daily_goal = round_int(monthly_goal as f64 / f64::from(days) / 10_000.0) * 10_000;
    let weekly_goal = round_int(monthly_goal as f64 / (f64::from(days) / 7.0) / 50_000.0) * 50_000;

    Smart {
        target_year: year,
        target_month: month,
        monthly: Monthly {
            goal: monthly_goal,
            last_year_revenue: round_num(last_year_revenue, 2),
            recent_3_month_avg: round_num(recent, 2),
            historical_avg: round_num(historical_avg, 2),
            yoy_goal: round_float(yoy_goal, 2),
            recent_goal: round_float(recent_goal, 2),
            ml_forecast_goal: round_float(ml, 2),
            growth_rate: round_float(growth_rate, 4),
            growth_cap: round_float(cap, 4),
            seasonality_index: index,
            confidence,
            calculation_method: method,
            blend_weights: if blend.is_empty() {
                None
            } else {
                Some(Ordered(
                    blend
                        .iter()
                        .map(|(k, v)| (*k, round_float(*v, 4)))
                        .collect(),
                ))
            },
        },
        weekly: Weekly {
            goal: weekly_goal,
            breakdown: Ordered(breakdown),
            weights: Ordered(weights),
        },
        daily: Daily {
            goal: daily_goal,
            days_in_month: days,
        },
        metadata: Metadata {
            overall_yoy: round_float(overall_yoy, 4),
            monthly_yoy: round_float(monthly_yoy, 4),
            calculated_at: kyiv_isoformat(now),
        },
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SmartPeriod {
    amount: PyNum,
    is_custom: bool,
    suggested_amount: i64,
    based_on_average: f64,
    trend: f64,
    confidence: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    weekly_breakdown: Option<Ordered<i64, i64>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SmartMonthly {
    amount: PyNum,
    is_custom: bool,
    suggested_amount: i64,
    based_on_average: PyNum,
    last_year_revenue: PyNum,
    #[serde(rename = "recent3MonthAvg")]
    recent_3_month_avg: PyNum,
    growth_rate: f64,
    seasonality_index: f64,
    trend: f64,
    confidence: String,
    calculation_method: &'static str,
    ml_forecast_goal: f64,
    blend_weights: Option<Ordered<&'static str, f64>>,
}

#[derive(Serialize)]
struct SmartGoals {
    daily: SmartPeriod,
    weekly: SmartPeriod,
    monthly: SmartMonthly,
    metadata: Metadata,
}

/// `get_smart_goals`: the smart goal for a month, this Kyiv month unless
/// asked, under any goal a person set by hand.
async fn smart_goals(
    store: &PgReads,
    q: &Query,
    sales_type: &str,
    now: DateTime<Utc>,
) -> Result<SmartGoals, Failure> {
    let local = now.with_timezone(&Kyiv);
    let year = q.year.unwrap_or_else(|| i64::from(local.year()));
    let month = q.month.unwrap_or_else(|| i64::from(local.month()));
    let s = smart(store, year, month, sales_type, now).await?;
    let stored = stored_goals(store).await?;
    // `{period: amount for rows if is_custom}`: the last such row wins.
    let custom = |period: &str| {
        stored
            .iter()
            .rev()
            .find(|(p, _, c)| p == period && c.unwrap_or(false))
            .map(|(_, a, _)| *a)
    };
    let days = f64::from(s.daily.days_in_month);
    let historical = s.monthly.historical_avg.as_f64();
    let trend = s.metadata.monthly_yoy * 100.0;
    let amount = |period: &str, goal: i64| custom(period).map_or(PyNum::Int(goal), PyNum::Float);
    Ok(SmartGoals {
        daily: SmartPeriod {
            amount: amount("daily", s.daily.goal),
            is_custom: custom("daily").is_some(),
            suggested_amount: s.daily.goal,
            based_on_average: historical / days,
            trend,
            confidence: s.monthly.confidence.clone(),
            weekly_breakdown: None,
        },
        weekly: SmartPeriod {
            amount: amount("weekly", s.weekly.goal),
            is_custom: custom("weekly").is_some(),
            suggested_amount: s.weekly.goal,
            based_on_average: historical / (days / 7.0),
            trend,
            confidence: s.monthly.confidence.clone(),
            weekly_breakdown: Some(Ordered(s.weekly.breakdown.0.clone())),
        },
        monthly: SmartMonthly {
            amount: amount("monthly", s.monthly.goal),
            is_custom: custom("monthly").is_some(),
            suggested_amount: s.monthly.goal,
            based_on_average: s.monthly.historical_avg,
            last_year_revenue: s.monthly.last_year_revenue,
            recent_3_month_avg: s.monthly.recent_3_month_avg,
            growth_rate: s.monthly.growth_rate,
            seasonality_index: s.monthly.seasonality_index,
            trend,
            confidence: s.monthly.confidence.clone(),
            calculation_method: s.monthly.calculation_method,
            ml_forecast_goal: s.monthly.ml_forecast_goal,
            blend_weights: s.monthly.blend_weights,
        },
        metadata: s.metadata,
    })
}

fn json<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).expect("the body is plain data")
}

/// The JSON body for a 200, or the reason there is none. `admit` is the
/// route's rate limit, which slowapi checks once the parameters parse and
/// before the route's own checks run.
pub async fn answer(
    route: Route,
    store: &PgReads,
    raw_query: &str,
    now: DateTime<Utc>,
    admit: impl FnOnce() -> Result<(), Failure>,
) -> Result<Vec<u8>, Failure> {
    let q = parse(route, raw_query)?;
    admit()?;
    if route == Route::History {
        let period = q.period_type.as_deref().unwrap_or_default();
        if !matches!(period, "daily" | "weekly" | "monthly") {
            return Err(Failure::BadRequest(
                "period_type must be: daily, weekly, or monthly".to_owned(),
            ));
        }
    }
    let sales_type = validate::sales_type(&q.sales_type).map_err(Failure::BadRequest)?;
    let today = now.with_timezone(&Kyiv).date_naive();
    match route {
        Route::Goals => Ok(json(&goals(store, &sales_type).await?)),
        Route::History => {
            let period = q.period_type.as_deref().unwrap_or_default();
            Ok(json(
                &history(store, period, q.weeks_back, &sales_type).await?,
            ))
        }
        Route::Smart => Ok(json(&smart_goals(store, &q, &sales_type, now).await?)),
        Route::Seasonality => Ok(json(&seasonality(store, &sales_type).await?)),
        Route::Growth => Ok(json(&growth(store, &sales_type, today).await?)),
        Route::Weekly => Ok(json(&weekly(store, &sales_type).await?)),
        Route::Forecast => {
            if q.recalculate {
                return Err(Failure::BadRequest(
                    "recalculate is not accepted on a GET: POST /api/goals/recalculate \
                     recomputes and stores the seasonality tables (admin)"
                        .to_owned(),
                ));
            }
            let (year, month) = (q.year.unwrap_or(0), q.month.unwrap_or(0));
            Ok(json(&smart(store, year, month, &sales_type, now).await?))
        }
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
            kinds(Route::Forecast, "recalculate=maybe"),
            [
                ("missing", "year"),
                ("missing", "month"),
                ("bool_parsing", "recalculate")
            ]
        );
        assert_eq!(
            kinds(Route::Forecast, "year=2019&month=13"),
            [("greater_than_equal", "year"), ("less_than_equal", "month")]
        );
        assert_eq!(
            kinds(Route::History, "weeks_back=0"),
            [
                ("missing", "period_type"),
                ("greater_than_equal", "weeks_back")
            ]
        );
        assert_eq!(
            kinds(Route::Smart, "month=x&year=2031"),
            [("less_than_equal", "year"), ("int_parsing", "month")]
        );
        assert!(kinds(Route::Goals, "year=x&month=y&weeks_back=z").is_empty());
        assert!(kinds(Route::Smart, "year=2026.0&month=%209").is_empty());
    }

    #[test]
    fn python_rounds_and_picks_this_way() {
        assert_eq!(round_int(2.5), 2);
        assert_eq!(round_int(3.5), 4);
        assert_eq!(round_int(-0.5), 0);
        assert_eq!(py_min(0.5, 0.5), 0.5);
        assert_eq!(recency_weighted(&[0.2]), 0.2);
        assert_eq!(days_in_month(2024, 2), 29);
        assert_eq!(days_in_month(2026, 12), 31);
    }

    #[test]
    fn an_ordered_map_is_a_dict() {
        let mut d: Ordered<i64, f64> = Ordered::new();
        d.set(3, 0.5);
        d.set(1, 0.25);
        d.setdefault(3, 9.0);
        d.setdefault(2, 0.23);
        assert_eq!(
            serde_json::to_string(&d).unwrap(),
            r#"{"3":0.5,"1":0.25,"2":0.23}"#
        );
    }

    #[test]
    fn calculated_at_is_kyivs_isoformat() {
        let t = DateTime::parse_from_rfc3339("2026-10-21T09:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(kyiv_isoformat(t), "2026-10-21T12:00:00+03:00");
        let t = DateTime::parse_from_rfc3339("2026-12-31T22:00:00.25Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(kyiv_isoformat(t), "2027-01-01T00:00:00.250000+02:00");
    }
}
