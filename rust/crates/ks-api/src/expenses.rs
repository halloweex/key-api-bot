//! The expenses router's four GET routes — `/api/expense-types`,
//! `/api/expenses/summary`, `/api/expenses/profit` and `/api/expenses` —
//! answered as `web/routes/api/expenses.py` answers them under
//! `KS_READ_EXPENSES=postgres`: the statements `core/repositories/expenses.py`
//! renders for Postgres, `pg_expenses_read.backfilled()`'s gate before the
//! two that read `bronze.expenses`, the Python arithmetic over their rows
//! (a day with no row is the int `0`, `sum()` is compensated), and the bytes
//! orjson writes.
//!
//! The parity is by contract: the summary's and the profit analysis' hooks
//! send the header's period, sales type and filters, of which the routes read
//! the source and ignore the rest alike; the type list is asked bare; the
//! manual list gets the filter store's period as it stands — `custom`
//! included — a category, and a limit. The integers are read as pydantic
//! reads them, as far as an `i64` reaches; where Python answers otherwise
//! past that, the harness's list says so.

use std::collections::HashMap;

use chrono::{DateTime, Datelike, NaiveDate, TimeDelta, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;
use sqlx::Row;
use sqlx::postgres::PgRow;

use crate::gold::decimal_to_float;
use crate::params::{FieldError, PyInt, int_message, lax_int};
use crate::period;
use crate::pgread::{Bind, PgReads};
use crate::pyrt::{PyNum, py_sum, round_float, round_num};
use crate::trend::{Failure, ReadError};
use crate::validate;

/// The surface `read_fallback` names in a 503.
const SURFACE: &str = "expenses";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    Types,
    Summary,
    Profit,
    List,
}

impl Route {
    /// The route for a contract endpoint name, if it is one of these.
    pub fn from_endpoint(name: &str) -> Option<Self> {
        Some(match name {
            "web.routes.api.expenses.get_expense_types" => Self::Types,
            "web.routes.api.expenses.get_expense_summary" => Self::Summary,
            "web.routes.api.expenses.get_profit_analysis" => Self::Profit,
            "web.routes.api.expenses.get_expenses" => Self::List,
            _ => return None,
        })
    }
}

// ─── The parameters ─────────────────────────────────────────────────────────

/// The route's parameters after FastAPI, before its own validators.
struct Query {
    period: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
    source_id: Option<i64>,
    /// Kept past 64 bits: `validate_db_int` refuses it with its value.
    expense_type_id: Option<PyInt>,
    sales_type: String,
    category: Option<String>,
    limit: i64,
}

fn field_error(
    kind: &'static str,
    name: &'static str,
    msg: &'static str,
    raw: String,
) -> FieldError {
    FieldError {
        kind,
        loc: ("query", name),
        msg,
        input: Some(raw),
    }
}

/// pydantic's lax `int`, as far as an `i64` reaches. Past 64 bits Rust reads
/// no integer, where Python's has no ceiling.
fn int(raw: String, name: &'static str, errors: &mut Vec<FieldError>) -> Option<i64> {
    let kind = match lax_int(&raw) {
        Ok(PyInt::Small(v)) => return Some(v),
        Ok(PyInt::Big(_)) => "int_parsing",
        Err(kind) => kind,
    };
    errors.push(field_error(kind, name, int_message(kind), raw));
    None
}

/// As `int`, but past 64 bits the digits Python's int would hold, for
/// `validate_db_int` to refuse as Python's does (#335).
fn int_or_big(raw: String, name: &'static str, errors: &mut Vec<FieldError>) -> Option<PyInt> {
    match lax_int(&raw) {
        Ok(v) => Some(v),
        Err(kind) => {
            errors.push(field_error(kind, name, int_message(kind), raw));
            None
        }
    }
}

/// `limit: int = Query(50, ge=1, le=200)`. pydantic's `ctx` is not carried:
/// only these hostile 422s would show it, and they are compared by status.
fn limit(raw: Option<String>, errors: &mut Vec<FieldError>) -> i64 {
    let Some(raw) = raw else { return 50 };
    let Some(value) = int(raw.clone(), "limit", errors) else {
        return 50;
    };
    if value < 1 {
        errors.push(field_error(
            "greater_than_equal",
            "limit",
            "Input should be greater than or equal to 1",
            raw,
        ));
    } else if value > 200 {
        errors.push(field_error(
            "less_than_equal",
            "limit",
            "Input should be less than or equal to 200",
            raw,
        ));
    }
    value
}

fn parse(route: Route, raw: &str) -> Result<Query, Failure> {
    let mut last = HashMap::new();
    for (name, value) in form_urlencoded::parse(raw.as_bytes()) {
        last.insert(name.into_owned(), value.into_owned());
    }
    let mut take = |name: &str| last.remove(name);
    let mut errors = Vec::new();
    // FastAPI reads the parameters in each signature's order.
    let windowed = matches!(route, Route::Summary | Route::Profit);
    let period = take("period");
    let (start_date, end_date) = if windowed {
        (take("start_date"), take("end_date"))
    } else {
        (None, None)
    };
    let source_id = match (windowed, take("source_id")) {
        (true, Some(raw)) => int(raw, "source_id", &mut errors),
        _ => None,
    };
    let expense_type_id = match (route, take("expense_type_id")) {
        (Route::Summary, Some(raw)) => int_or_big(raw, "expense_type_id", &mut errors),
        _ => None,
    };
    let sales_type = take("sales_type").unwrap_or_else(|| "retail".to_owned());
    let (category, limit) = match route {
        Route::List => (take("category"), limit(take("limit"), &mut errors)),
        _ => (None, 50),
    };
    if !errors.is_empty() {
        return Err(Failure::Unprocessable(errors));
    }
    Ok(Query {
        period,
        start_date,
        end_date,
        source_id,
        expense_type_id,
        sales_type,
        category,
        limit,
    })
}

/// What the summary's and the profit analysis' validators leave: period,
/// source and sales type, in that order, then the dates.
struct Window {
    start: NaiveDate,
    end: NaiveDate,
    source_id: Option<i64>,
    sales_type: String,
}

fn window(q: &Query, now: DateTime<Utc>) -> Result<Window, Failure> {
    let bad = Failure::BadRequest;
    validate::period(q.period.as_deref()).map_err(bad)?;
    validate::source_id(q.source_id.map(PyInt::Small).as_ref()).map_err(bad)?;
    // `validate_db_int(expense_type_id, "expense_type_id")`: the summary's,
    // before the sales type; the profit route reads no expense type.
    if let Some(refused) = validate::db_int(q.expense_type_id.as_ref(), "expense_type_id", 32) {
        return Err(bad(refused));
    }
    let sales_type = validate::sales_type(&q.sales_type).map_err(bad)?;
    let (start, end) = period::parse_period(
        q.period.as_deref(),
        q.start_date.as_deref(),
        q.end_date.as_deref(),
        period::kyiv_today(now),
    )?;
    Ok(Window {
        start,
        end,
        source_id: q.source_id,
        sales_type,
    })
}

fn first_of_month(d: NaiveDate) -> NaiveDate {
    d.with_day(1).expect("every month has a first day")
}

fn days_back(d: NaiveDate, n: i64) -> Result<NaiveDate, Failure> {
    period::add_days(d, -n).map_err(Failure::from)
}

/// `GET /api/expenses`' own reading of `period`, not `parse_period`'s: six of
/// the eight shortcuts, exactly as spelled. Anything else — `last_7_days`,
/// `last_28_days`, `custom`, a typo, an empty value — is no window at all.
fn list_window(
    period: Option<&str>,
    today: NaiveDate,
) -> Result<Option<(NaiveDate, NaiveDate)>, Failure> {
    let weekday = i64::from(today.weekday().num_days_from_monday());
    Ok(match period {
        Some("today") => Some((today, today)),
        Some("yesterday") => {
            let day = days_back(today, 1)?;
            Some((day, day))
        }
        Some("week") => Some((days_back(today, weekday)?, today)),
        Some("last_week") => {
            let end = days_back(today, weekday + 1)?;
            Some((days_back(end, 6)?, end))
        }
        Some("month") => Some((first_of_month(today), today)),
        Some("last_month") => {
            let end = days_back(first_of_month(today), 1)?;
            Some((first_of_month(end), end))
        }
        _ => None,
    })
}

// ─── Reading a row ──────────────────────────────────────────────────────────

/// An integer column whatever Postgres typed it, as `int()` reads it.
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

/// `float(x)` of a numeric column; a NULL is Python's TypeError, a 500.
fn float_req(row: &PgRow, i: usize) -> Result<f64, Failure> {
    let v: Option<Decimal> = row
        .try_get(i)
        .map_err(|_| Failure::Raised("a column the statement sums is not a numeric"))?;
    v.map(decimal_to_float)
        .ok_or(Failure::Raised("float(None)"))
}

fn text(row: &PgRow, i: usize) -> Result<Option<String>, Failure> {
    row.try_get(i)
        .map_err(|_| Failure::Raised("a column the statement names is not text"))
}

/// `datetime.isoformat()` of an aware UTC datetime, as asyncpg hands one
/// over: microseconds only when there are any, and the offset spelled out.
fn isoformat(t: DateTime<Utc>) -> String {
    let base = t.format("%Y-%m-%dT%H:%M:%S");
    match t.timestamp_subsec_micros() {
        0 => format!("{base}+00:00"),
        micros => format!("{base}.{micros:06}+00:00"),
    }
}

// ─── The reads ──────────────────────────────────────────────────────────────

async fn rows(store: &PgReads, sql: String, binds: &[Bind]) -> Result<Vec<PgRow>, Failure> {
    store
        .rows(sql, binds)
        .await
        .map_err(|e| Failure::Unavailable(SURFACE, e))
}

/// `pg_expenses_read.backfilled()`, asked before a statement that reads
/// `bronze.expenses`: without its history Postgres would answer almost no
/// costs, and under `KS_DUCKDB=off` no other engine may (`no_engine`).
async fn history(store: &PgReads) -> Result<(), Failure> {
    let found = rows(
        store,
        "SELECT backfilled_at IS NOT NULL FROM meta.mirror_state WHERE table_name = $1".to_owned(),
        &[Bind::Text("bronze.expenses".to_owned())],
    )
    .await?;
    let done = match found.first() {
        Some(row) => row
            .try_get::<Option<bool>, _>(0)
            .map_err(|_| Failure::Raised("a flag that is not a boolean"))?,
        None => None,
    };
    match done {
        Some(true) => Ok(()),
        _ => Err(Failure::Unavailable(
            SURFACE,
            ReadError::Failed("bronze.expenses holds no backfilled history in Postgres".to_owned()),
        )),
    }
}

#[derive(Serialize)]
struct ExpenseType {
    id: i64,
    name: Option<String>,
    alias: Option<String>,
}

/// `get_expense_types`.
async fn types(store: &PgReads) -> Result<Vec<ExpenseType>, Failure> {
    let found = rows(
        store,
        "
            SELECT id, name, alias, is_active
            FROM bronze.expense_types
            WHERE is_active = TRUE
            ORDER BY name, id
        "
        .to_owned(),
        &[],
    )
    .await?;
    let mut out = Vec::with_capacity(found.len());
    for row in &found {
        out.push(ExpenseType {
            id: int_col(row, 0)?.ok_or(Failure::Raised("an id that is NULL"))?,
            name: text(row, 1)?,
            alias: text(row, 2)?,
        });
    }
    Ok(out)
}

/// The order-side WHERE of the summary and the profit analysis, and its binds.
fn order_where(w: &Window, returns: bool, expense_type_id: Option<i64>) -> (String, Vec<Bind>) {
    let mut binds = vec![Bind::Date(w.start), Bind::Date(w.end)];
    let mut clauses = vec!["s.order_date BETWEEN $1 AND $2".to_owned()];
    if !returns {
        clauses.push("NOT s.is_return".to_owned());
    }
    if w.sales_type != "all" {
        let at = next(&mut binds, Bind::Text(w.sales_type.clone()));
        clauses.push(format!("s.sales_type = {at}"));
    }
    // `if source_id:` and `if expense_type_id:` — a zero is no filter.
    if let Some(source) = w.source_id.filter(|v| *v != 0) {
        let at = next(&mut binds, Bind::Int(source));
        clauses.push(format!("s.source_id = {at}"));
    }
    if let Some(kind) = expense_type_id.filter(|v| *v != 0) {
        let at = next(&mut binds, Bind::Int(kind));
        clauses.push(format!("e.expense_type_id = {at}"));
    }
    (clauses.join(" AND "), binds)
}

/// The next `$n`, for a parameter appended in placeholder order.
fn next(binds: &mut Vec<Bind>, bind: Bind) -> String {
    binds.push(bind);
    format!("${}", binds.len())
}

/// Every day of the range, in order; none for a range that runs backwards.
fn days(start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
    let mut out = Vec::new();
    let mut current = start;
    while current <= end {
        out.push(current);
        match current.checked_add_signed(TimeDelta::days(1)) {
            Some(next) => current = next,
            None => break,
        }
    }
    out
}

/// `strftime("%d.%m")`.
fn label(d: NaiveDate) -> String {
    d.format("%d.%m").to_string()
}

/// `expense_colors`.
const EXPENSE_COLORS: [&str; 8] = [
    "#EF4444", "#F59E0B", "#8B5CF6", "#06B6D4", "#EC4899", "#14B8A6", "#7C3AED", "#2563EB",
];

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ByType {
    labels: Vec<String>,
    data: Vec<f64>,
    counts: Vec<i64>,
    background_color: Vec<&'static str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SummaryDataset {
    label: &'static str,
    data: Vec<PyNum>,
    border_color: &'static str,
    background_color: &'static str,
    fill: bool,
    tension: f64,
    border_width: i64,
}

#[derive(Serialize)]
struct SummaryTrend {
    labels: Vec<String>,
    datasets: [SummaryDataset; 1],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SummaryMetrics {
    total_expenses: f64,
    expense_count: i64,
    orders_with_expenses: i64,
    avg_expense_per_order: PyNum,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Summary {
    by_type: ByType,
    trend: SummaryTrend,
    metrics: SummaryMetrics,
}

const EXPENSES_JOIN: &str = "FROM bronze.expenses e
            JOIN silver.orders s ON e.order_id = s.id";

/// `get_expense_summary`.
async fn summary(
    store: &PgReads,
    w: &Window,
    expense_type_id: Option<i64>,
) -> Result<Summary, Failure> {
    let (wh, binds) = order_where(w, true, expense_type_id);
    history(store).await?;
    let by_type = rows(
        store,
        format!(
            "
            SELECT
                COALESCE(et.name, 'Other') as type_name,
                et.id as type_id,
                SUM(e.amount) as total_amount,
                COUNT(e.id) as expense_count
            {EXPENSES_JOIN}
            LEFT JOIN bronze.expense_types et ON e.expense_type_id = et.id
            WHERE {wh}
            GROUP BY et.id, et.name
            ORDER BY total_amount DESC, et.id
        "
        ),
        &binds,
    )
    .await?;
    let totals = rows(
        store,
        format!(
            "
            SELECT
                COALESCE(SUM(e.amount), 0) as total_expenses,
                COUNT(e.id) as expense_count,
                COUNT(DISTINCT e.order_id) as orders_with_expenses
            {EXPENSES_JOIN}
            WHERE {wh}
        "
        ),
        &binds,
    )
    .await?;
    let trend = rows(
        store,
        format!(
            "
            SELECT
                s.order_date as day,
                SUM(e.amount) as total_expenses
            {EXPENSES_JOIN}
            WHERE {wh}
            GROUP BY s.order_date
            ORDER BY day
        "
        ),
        &binds,
    )
    .await?;

    let mut daily = HashMap::with_capacity(trend.len());
    for row in &trend {
        let day: NaiveDate = row
            .try_get(0)
            .map_err(|_| Failure::Raised("a day that is not a date"))?;
        daily.insert(day, float_req(row, 1)?);
    }
    let range = days(w.start, w.end);
    // `round(daily_data.get(current, 0), 2)`: a day with no row stays the int.
    let data = range
        .iter()
        .map(|d| {
            daily
                .get(d)
                .map_or(PyNum::Int(0), |v| PyNum::Float(round_float(*v, 2)))
        })
        .collect();

    let mut types = ByType {
        labels: Vec::with_capacity(by_type.len()),
        data: Vec::with_capacity(by_type.len()),
        counts: Vec::with_capacity(by_type.len()),
        background_color: EXPENSE_COLORS.iter().take(by_type.len()).copied().collect(),
    };
    for row in &by_type {
        types.labels.push(text(row, 0)?.unwrap_or_default());
        types.data.push(round_float(float_req(row, 2)?, 2));
        types.counts.push(int_col(row, 3)?.unwrap_or(0));
    }

    let total = totals
        .first()
        .ok_or(Failure::Raised("an aggregate answered no row"))?;
    // `float(total_result[0] or 0)`, `total_result[1] or 0`, `… [2] or 0`.
    let total_expenses = float_req(total, 0)?;
    let expense_count = int_col(total, 1)?.unwrap_or(0);
    let orders_with_expenses = int_col(total, 2)?.unwrap_or(0);
    Ok(Summary {
        by_type: types,
        trend: SummaryTrend {
            labels: range.iter().map(|d| label(*d)).collect(),
            datasets: [SummaryDataset {
                label: "Expenses (UAH)",
                data,
                border_color: "#EF4444",
                background_color: "rgba(239, 68, 68, 0.1)",
                fill: true,
                tension: 0.3,
                border_width: 2,
            }],
        },
        metrics: SummaryMetrics {
            total_expenses: round_float(total_expenses, 2),
            expense_count,
            orders_with_expenses,
            avg_expense_per_order: if orders_with_expenses > 0 {
                PyNum::Float(round_float(total_expenses / orders_with_expenses as f64, 2))
            } else {
                PyNum::Int(0)
            },
        },
    })
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProfitDataset {
    label: &'static str,
    data: Vec<PyNum>,
    border_color: &'static str,
    background_color: &'static str,
    fill: bool,
    tension: f64,
}

#[derive(Serialize)]
struct ProfitChart {
    labels: Vec<String>,
    datasets: [ProfitDataset; 3],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProfitMetrics {
    total_revenue: PyNum,
    total_expenses: PyNum,
    gross_profit: PyNum,
    profit_margin: PyNum,
}

#[derive(Serialize)]
struct Profit {
    chart: ProfitChart,
    metrics: ProfitMetrics,
}

/// `a - b` as Python computes it on an int-or-float.
fn minus(a: PyNum, b: PyNum) -> PyNum {
    match (a, b) {
        (PyNum::Int(x), PyNum::Int(y)) => PyNum::Int(x - y),
        _ => PyNum::Float(a.as_f64() - b.as_f64()),
    }
}

fn dataset(
    label: &'static str,
    data: Vec<PyNum>,
    border_color: &'static str,
    background_color: &'static str,
    fill: bool,
) -> ProfitDataset {
    ProfitDataset {
        label,
        data,
        border_color,
        background_color,
        fill,
        tension: 0.3,
    }
}

/// `get_profit_analysis`: the expenses folded per order before the join, so
/// an order's revenue is counted once however many expenses it carries.
async fn profit(store: &PgReads, w: &Window) -> Result<Profit, Failure> {
    let (wh, binds) = order_where(w, false, None);
    history(store).await?;
    let found = rows(
        store,
        format!(
            "
            SELECT
                s.order_date as day,
                SUM(s.grand_total) as revenue,
                COALESCE(SUM(x.amount), 0) as expenses
            FROM silver.orders s
            LEFT JOIN (
                SELECT order_id, SUM(amount) AS amount
                FROM bronze.expenses
                GROUP BY order_id
            ) x ON x.order_id = s.id
            WHERE {wh}
            GROUP BY s.order_date
            ORDER BY day
        "
        ),
        &binds,
    )
    .await?;
    let mut daily = HashMap::with_capacity(found.len());
    for row in &found {
        let day: NaiveDate = row
            .try_get(0)
            .map_err(|_| Failure::Raised("a day that is not a date"))?;
        daily.insert(day, (float_req(row, 1)?, float_req(row, 2)?));
    }
    let range = days(w.start, w.end);
    let (mut revenue, mut expenses, mut gross) = (Vec::new(), Vec::new(), Vec::new());
    for day in &range {
        // `daily_data.get(current, {"revenue": 0, "expenses": 0})`.
        match daily.get(day) {
            Some(&(r, e)) => {
                revenue.push(PyNum::Float(round_float(r, 2)));
                expenses.push(PyNum::Float(round_float(e, 2)));
                gross.push(PyNum::Float(round_float(r - e, 2)));
            }
            None => {
                revenue.push(PyNum::Int(0));
                expenses.push(PyNum::Int(0));
                gross.push(PyNum::Int(0));
            }
        }
    }
    let total_revenue = py_sum(&revenue);
    let total_expenses = py_sum(&expenses);
    let total_profit = minus(total_revenue, total_expenses);
    let margin = if total_revenue.as_f64() > 0.0 {
        PyNum::Float(total_profit.as_f64() / total_revenue.as_f64() * 100.0)
    } else {
        PyNum::Int(0)
    };
    Ok(Profit {
        chart: ProfitChart {
            labels: range.iter().map(|d| label(*d)).collect(),
            datasets: [
                dataset(
                    "Revenue",
                    revenue,
                    "#16A34A",
                    "rgba(22, 163, 74, 0.1)",
                    false,
                ),
                dataset(
                    "Expenses",
                    expenses,
                    "#EF4444",
                    "rgba(239, 68, 68, 0.1)",
                    false,
                ),
                dataset(
                    "Gross Profit",
                    gross,
                    "#2563EB",
                    "rgba(37, 99, 235, 0.2)",
                    true,
                ),
            ],
        },
        metrics: ProfitMetrics {
            total_revenue: round_num(total_revenue, 2),
            total_expenses: round_num(total_expenses, 2),
            gross_profit: round_num(total_profit, 2),
            profit_margin: round_num(margin, 1),
        },
    })
}

#[derive(Serialize)]
struct ManualExpense {
    id: i64,
    expense_date: Option<String>,
    category: Option<String>,
    expense_type: Option<String>,
    amount: f64,
    currency: Option<String>,
    note: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
    platform: Option<String>,
}

#[derive(Serialize)]
struct ByCategory {
    category: Option<String>,
    total: f64,
    count: i64,
}

#[derive(Serialize)]
struct ManualSummary {
    total: f64,
    count: i64,
    by_category: Vec<ByCategory>,
}

#[derive(Serialize)]
struct ManualList {
    expenses: Vec<ManualExpense>,
    summary: ManualSummary,
    period: Option<String>,
    category: Option<String>,
}

/// The manual list's WHERE: the window's two bounds, then the category.
fn manual_where(
    window: Option<(NaiveDate, NaiveDate)>,
    category: Option<&str>,
) -> (String, Vec<Bind>) {
    let mut binds = Vec::new();
    let mut conditions = Vec::new();
    if let Some((start, end)) = window {
        binds.push(Bind::Date(start));
        conditions.push(format!("expense_date >= ${}", binds.len()));
        binds.push(Bind::Date(end));
        conditions.push(format!("expense_date <= ${}", binds.len()));
    }
    if let Some(category) = category.filter(|c| !c.is_empty()) {
        binds.push(Bind::Text(category.to_owned()));
        conditions.push(format!("category = ${}", binds.len()));
    }
    let clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };
    (clause, binds)
}

/// `GET /api/expenses`: `list_expenses`, then `get_expenses_summary` over the
/// same window and every category.
async fn manual(store: &PgReads, q: &Query, now: DateTime<Utc>) -> Result<ManualList, Failure> {
    let window = list_window(q.period.as_deref(), period::kyiv_today(now))?;
    let (wh, mut binds) = manual_where(window, q.category.as_deref());
    binds.push(Bind::Int(q.limit));
    let listed = rows(
        store,
        format!(
            "
            SELECT id, expense_date, category, expense_type, amount, currency, note, created_at, updated_at, platform
            FROM app.manual_expenses
            {wh}
            ORDER BY expense_date DESC, created_at DESC, id DESC
            LIMIT ${}
        ",
            binds.len()
        ),
        &binds,
    )
    .await?;
    let mut expenses = Vec::with_capacity(listed.len());
    for row in &listed {
        let date: Option<NaiveDate> = row
            .try_get(1)
            .map_err(|_| Failure::Raised("an expense date that is not a date"))?;
        let stamp = |i: usize| -> Result<Option<String>, Failure> {
            let t: Option<DateTime<Utc>> = row
                .try_get(i)
                .map_err(|_| Failure::Raised("a stamp that is not a timestamptz"))?;
            Ok(t.map(isoformat))
        };
        expenses.push(ManualExpense {
            id: int_col(row, 0)?.ok_or(Failure::Raised("an id that is NULL"))?,
            expense_date: date.map(|d| d.format("%Y-%m-%d").to_string()),
            category: text(row, 2)?,
            expense_type: text(row, 3)?,
            amount: float_req(row, 4)?,
            currency: text(row, 5)?,
            note: text(row, 6)?,
            created_at: stamp(7)?,
            updated_at: stamp(8)?,
            platform: text(row, 9)?,
        });
    }

    let (wh, binds) = manual_where(window, None);
    let total = rows(
        store,
        format!(
            "
            SELECT COALESCE(SUM(amount), 0) as total, COUNT(*) as count
            FROM app.manual_expenses
            {wh}
        "
        ),
        &binds,
    )
    .await?;
    let by_category = rows(
        store,
        format!(
            "
            SELECT category, SUM(amount) as total, COUNT(*) as count
            FROM app.manual_expenses
            {wh}
            GROUP BY category
            ORDER BY total DESC, category
        "
        ),
        &binds,
    )
    .await?;
    let total = total
        .first()
        .ok_or(Failure::Raised("an aggregate answered no row"))?;
    let mut categories = Vec::with_capacity(by_category.len());
    for row in &by_category {
        categories.push(ByCategory {
            category: text(row, 0)?,
            total: float_req(row, 1)?,
            count: int_col(row, 2)?.unwrap_or(0),
        });
    }
    Ok(ManualList {
        expenses,
        summary: ManualSummary {
            total: float_req(total, 0)?,
            count: int_col(total, 1)?.unwrap_or(0),
            by_category: categories,
        },
        period: q.period.clone(),
        category: q.category.clone(),
    })
}

fn json<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).expect("the body is plain data")
}

/// The JSON body for a 200, or the reason there is none. `admit` is the
/// route's rate limit, which slowapi checks once the parameters parse and
/// before the route's own validators run.
pub async fn answer(
    route: Route,
    store: &PgReads,
    raw_query: &str,
    now: DateTime<Utc>,
    admit: impl FnOnce() -> Result<(), Failure>,
) -> Result<Vec<u8>, Failure> {
    let q = parse(route, raw_query)?;
    admit()?;
    match route {
        Route::Types => Ok(json(&types(store).await?)),
        Route::Summary => {
            let w = window(&q, now)?;
            let expense_type_id = match q.expense_type_id {
                Some(PyInt::Small(v)) => Some(v),
                _ => None,
            };
            Ok(json(&summary(store, &w, expense_type_id).await?))
        }
        Route::Profit => {
            let w = window(&q, now)?;
            Ok(json(&profit(store, &w).await?))
        }
        Route::List => Ok(json(&manual(store, &q, now).await?)),
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
    fn each_route_reads_only_what_it_declares() {
        assert_eq!(
            kinds(Route::Summary, "expense_type_id=x&source_id=y"),
            [
                ("int_parsing", "source_id"),
                ("int_parsing", "expense_type_id")
            ]
        );
        assert!(kinds(Route::Profit, "expense_type_id=x&limit=y&category=z").is_empty());
        assert!(kinds(Route::Types, "source_id=x&limit=0").is_empty());
        assert_eq!(
            kinds(Route::List, "limit=0"),
            [("greater_than_equal", "limit")]
        );
        assert_eq!(
            kinds(Route::List, "limit=201"),
            [("less_than_equal", "limit")]
        );
        assert_eq!(
            kinds(Route::List, "limit=x&source_id=y"),
            [("int_parsing", "limit")]
        );
        assert!(kinds(Route::List, "limit=%201_0").is_empty());
    }

    #[test]
    fn the_list_knows_six_shortcuts_and_nothing_else() {
        let wed = NaiveDate::from_ymd_opt(2026, 10, 21).unwrap();
        let day = |m, d| NaiveDate::from_ymd_opt(2026, m, d).unwrap();
        assert_eq!(
            list_window(Some("week"), wed).unwrap(),
            Some((day(10, 19), wed))
        );
        assert_eq!(
            list_window(Some("last_week"), wed).unwrap(),
            Some((day(10, 12), day(10, 18)))
        );
        assert_eq!(
            list_window(Some("last_month"), wed).unwrap(),
            Some((day(9, 1), day(9, 30)))
        );
        for unknown in [
            "last_7_days",
            "last_28_days",
            "custom",
            "MONTH",
            " month",
            "",
        ] {
            assert_eq!(list_window(Some(unknown), wed).unwrap(), None, "{unknown}");
        }
    }

    #[test]
    fn isoformat_is_pythons() {
        let t = DateTime::parse_from_rfc3339("2026-10-21T09:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(isoformat(t), "2026-10-21T09:00:00+00:00");
        let t = DateTime::parse_from_rfc3339("2026-10-21T09:00:00.0005Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(isoformat(t), "2026-10-21T09:00:00.000500+00:00");
    }

    #[test]
    fn a_day_with_no_row_stays_an_int_through_the_totals() {
        assert_eq!(minus(PyNum::Int(0), PyNum::Int(0)), PyNum::Int(0));
        assert_eq!(minus(PyNum::Float(1.5), PyNum::Int(0)), PyNum::Float(1.5));
        let wed = NaiveDate::from_ymd_opt(2026, 10, 21).unwrap();
        assert!(days(wed, wed.pred_opt().unwrap()).is_empty());
        assert_eq!(label(wed), "21.10");
    }
}
