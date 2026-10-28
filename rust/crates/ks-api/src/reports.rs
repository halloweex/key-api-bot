//! `/api/reports/*`: the six GET routes behind /reports and /marketing,
//! answered as `web/routes/api/reports.py` answers them under
//! `KS_READ_REPORTS=postgres` and `KS_READ_MARKETING=postgres` — the
//! statements `core/repositories/revenue.py` renders for Postgres, the Python
//! arithmetic over their rows, the bytes orjson writes, and for the two CSV
//! downloads the bytes `csv.writer` and `encode("utf-8-sig")` write.
//!
//! The parity is by contract: what the two pages, their tabs and their CSV
//! links send (`tests/frontend_contract.py`) — the header's period, sales
//! type and filters, a source chip, a limit from the tab's options, a report
//! `type`. The three integers are read as pydantic reads them, as far as an
//! `i64` reaches; where Python answers otherwise past that, the harness's
//! list says so.

use std::cmp::Ordering;
use std::collections::HashMap;

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::Serialize;
use sqlx::Row;
use sqlx::postgres::PgRow;

use crate::gold::decimal_to_float;
use crate::params::{FieldError, PyInt, int_message, lax_int};
use crate::period;
use crate::pgread::{Bind, PgReads};
use crate::pyrt::{PyNum, py_strip, py_sum, round_float, round_num};
use crate::trend::Failure;
use crate::validate;

/// The surfaces `read_fallback` names in a 503.
const REPORTS: &str = "reports";
const MARKETING: &str = "marketing";
/// `_category_ids` rides `KS_READ_LOOKUPS`, whichever tab asks.
const LOOKUPS: &str = "lookups";
/// `source_names`: source 5 has none, and its rows are skipped.
const SOURCE_NAMES: [(i64, &str); 3] = [(1, "Instagram"), (2, "Telegram"), (4, "Shopify")];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    Summary,
    TopProducts,
    AllProducts,
    ExportCsv,
    Marketing,
    MarketingCsv,
}

impl Route {
    /// The route for a contract endpoint name, if it is one of these.
    pub fn from_endpoint(name: &str) -> Option<Self> {
        Some(match name {
            "web.routes.api.reports.get_report_summary" => Self::Summary,
            "web.routes.api.reports.get_report_top_products" => Self::TopProducts,
            "web.routes.api.reports.get_report_all_products" => Self::AllProducts,
            "web.routes.api.reports.export_report_csv" => Self::ExportCsv,
            "web.routes.api.reports.get_marketing_summary" => Self::Marketing,
            "web.routes.api.reports.export_marketing_csv" => Self::MarketingCsv,
            _ => return None,
        })
    }

    fn is_marketing(self) -> bool {
        matches!(self, Self::Marketing | Self::MarketingCsv)
    }
}

/// A 200's body: JSON, or a CSV with the name it is offered under.
pub enum Answer {
    Json(Vec<u8>),
    Csv { body: Vec<u8>, filename: String },
}

// ─── The parameters ─────────────────────────────────────────────────────────

/// The route's parameters after FastAPI, before its own validators.
struct Query {
    kind: Option<String>,
    period: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
    source_id: Option<i64>,
    category_id: Option<i64>,
    brand: Option<String>,
    sales_type: String,
    limit: i64,
}

/// pydantic's lax `int` — `" 5"`, `"1_0"` and `"5.0"` are integers to it, as
/// they are to the app — as far as an `i64` reaches. Past 64 bits Rust reads
/// no integer, where Python's has no ceiling.
fn int(raw: String, name: &'static str, errors: &mut Vec<FieldError>) -> Option<i64> {
    let kind = match lax_int(&raw) {
        Ok(PyInt::Small(v)) => return Some(v),
        Ok(PyInt::Big(_)) => "int_parsing",
        Err(kind) => kind,
    };
    errors.push(FieldError {
        kind,
        loc: ("query", name),
        msg: int_message(kind),
        input: Some(raw),
    });
    None
}

fn parse(route: Route, raw: &str) -> Result<Query, Failure> {
    let mut last = HashMap::new();
    for (name, value) in form_urlencoded::parse(raw.as_bytes()) {
        last.insert(name.into_owned(), value.into_owned());
    }
    let mut take = |name: &str| last.remove(name);
    let mut errors = Vec::new();
    // FastAPI reads the parameters in the signature's order. The export's
    // `type` comes first and is required.
    let kind = match (route, take("type")) {
        (Route::ExportCsv, None) => {
            errors.push(FieldError {
                kind: "missing",
                loc: ("query", "type"),
                msg: "Field required",
                input: None,
            });
            None
        }
        (Route::ExportCsv, kind) => kind,
        _ => None,
    };
    let period = take("period");
    let start_date = take("start_date");
    let end_date = take("end_date");
    let source_id = match (route.is_marketing(), take("source_id")) {
        (false, Some(raw)) => int(raw, "source_id", &mut errors),
        _ => None,
    };
    let category_id = take("category_id").and_then(|raw| int(raw, "category_id", &mut errors));
    let brand = take("brand");
    let sales_type = take("sales_type").unwrap_or_else(|| "retail".to_owned());
    let limit = match (route, take("limit")) {
        (Route::TopProducts | Route::ExportCsv, Some(raw)) => {
            int(raw, "limit", &mut errors).unwrap_or(10)
        }
        _ => 10,
    };
    if !errors.is_empty() {
        return Err(Failure::Unprocessable(errors));
    }
    Ok(Query {
        kind,
        period,
        start_date,
        end_date,
        source_id,
        category_id,
        brand,
        sales_type,
        limit,
    })
}

/// What the validators leave the route.
struct Window {
    start: NaiveDate,
    end: NaiveDate,
    source_id: Option<i64>,
    category_id: Option<i64>,
    brand: Option<String>,
    sales_type: String,
}

fn dates(q: &Query, now: DateTime<Utc>) -> Result<(NaiveDate, NaiveDate), Failure> {
    Ok(period::parse_period(
        q.period.as_deref(),
        q.start_date.as_deref(),
        q.end_date.as_deref(),
        period::kyiv_today(now),
    )?)
}

/// `_parse_common_params`: period, source, category, brand, sales type,
/// then the dates.
fn reports_window(q: &Query, now: DateTime<Utc>) -> Result<Window, Failure> {
    let bad = Failure::BadRequest;
    validate::period(q.period.as_deref()).map_err(bad)?;
    validate::source_id(q.source_id.map(PyInt::Small).as_ref()).map_err(bad)?;
    validate::category_id(q.category_id.map(PyInt::Small).as_ref()).map_err(bad)?;
    let brand = validate::brand_value(q.brand.as_deref()).map_err(bad)?;
    let sales_type = validate::sales_type(&q.sales_type).map_err(bad)?;
    let (start, end) = dates(q, now)?;
    Ok(Window {
        start,
        end,
        source_id: q.source_id,
        category_id: q.category_id,
        brand,
        sales_type,
    })
}

/// The marketing routes' own order: the sales type first, the period only
/// when one is given, then category and brand.
fn marketing_window(q: &Query, now: DateTime<Utc>) -> Result<Window, Failure> {
    let bad = Failure::BadRequest;
    let sales_type = validate::sales_type(&q.sales_type).map_err(bad)?;
    if q.period.as_deref().is_some_and(|p| !p.is_empty()) {
        validate::period(q.period.as_deref()).map_err(bad)?;
    }
    validate::category_id(q.category_id.map(PyInt::Small).as_ref()).map_err(bad)?;
    let brand = validate::brand_value(q.brand.as_deref()).map_err(bad)?;
    let (start, end) = dates(q, now)?;
    Ok(Window {
        start,
        end,
        source_id: None,
        category_id: q.category_id,
        brand,
        sales_type,
    })
}

/// `core.validators.validate_limit(value, max_value=…)`.
fn validate_limit(value: i64, max: i64) -> Result<i64, Failure> {
    if value < 1 {
        return Err(Failure::BadRequest(format!(
            "limit: Must be at least 1 (got: {value})"
        )));
    }
    if value > max {
        return Err(Failure::BadRequest(format!(
            "limit: Cannot exceed {max} (got: {value})"
        )));
    }
    Ok(value)
}

// ─── The statements ─────────────────────────────────────────────────────────
//
// `core/repositories/revenue.py`'s texts as `render_tables(sql, POSTGRES)`
// and `numbered()` leave them: a `?` becomes the next `$n` in the order the
// text holds them, which is the order the caller appends its parameters.

/// The parameters in placeholder order: each `next` is the next `$n`.
#[derive(Default)]
struct Args(Vec<Bind>);

impl Args {
    fn next(&mut self, bind: Bind) -> String {
        self.0.push(bind);
        format!("${}", self.0.len())
    }
}

/// `core.duckdb_constants.brand_where`, the unknown bucket included.
fn brand_where(brand: &str, args: &mut Args, alias: &str) -> String {
    let col = format!("{alias}.brand");
    // `brand.strip().casefold() == UNKNOWN_BRAND.casefold()`: no other text
    // casefolds to "unknown" but by lowering.
    if py_strip(brand).to_lowercase() == "unknown" {
        return format!("({col} IS NULL OR TRIM({col}) = '')");
    }
    let at = args.next(Bind::Text(brand.to_owned()));
    format!("LOWER({col}) = LOWER({at})")
}

/// The category branch, then the brand: the order every caller adds them.
fn product_filter(
    alias: &str,
    cats: Option<&[i64]>,
    brand: Option<&str>,
    args: &mut Args,
) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(cats) = cats {
        let at: Vec<String> = cats.iter().map(|c| args.next(Bind::Int(*c))).collect();
        out.push(format!("{alias}.category_id IN ({})", at.join(",")));
    }
    if let Some(brand) = brand {
        out.push(brand_where(brand, args, alias));
    }
    out
}

/// The date window and the sales type, on the order lines.
fn line_clauses(w: &Window, args: &mut Args) -> Vec<String> {
    let mut clauses = vec![
        format!(
            "l.order_date BETWEEN {} AND {}",
            args.next(Bind::Date(w.start)),
            args.next(Bind::Date(w.end))
        ),
        "NOT l.is_return".to_owned(),
        "l.is_active_source".to_owned(),
    ];
    if w.sales_type != "all" {
        let at = args.next(Bind::Text(w.sales_type.clone()));
        clauses.push(format!("l.sales_type = {at}"));
    }
    clauses
}

const TREE_SQL: &str = "
    WITH RECURSIVE category_tree AS (
        SELECT id FROM bronze.categories WHERE id = $1
        UNION ALL
        SELECT c.id FROM bronze.categories c
        JOIN category_tree ct ON c.parent_id = ct.id
    )
    SELECT id FROM category_tree
    ORDER BY id
";

/// `get_report_summary`'s two bodies: order-grain either way, narrowed
/// through the order lines when a product filter is on.
fn summary_sql(wh: &str, filtered: bool) -> String {
    let measures = |op: &str| {
        format!(
            "s.source_id,
                    COUNT(CASE WHEN NOT s.is_return THEN 1 END) as orders_count,
                    COALESCE(SUM(CASE WHEN NOT s.is_return THEN (
                        SELECT SUM({op}.quantity) FROM bronze.order_products {op} WHERE {op}.order_id = s.id
                    ) ELSE 0 END), 0) as products_sold,
                    COALESCE(SUM(CASE WHEN NOT s.is_return THEN s.grand_total ELSE 0 END), 0) as revenue,
                    COUNT(CASE WHEN s.is_return THEN 1 END) as returns_count"
        )
    };
    if filtered {
        format!(
            "
                WITH matching_orders AS (
                SELECT DISTINCT s.order_id AS id
                FROM silver.order_lines s
                WHERE {wh}
            )
                SELECT
                    {}
                FROM silver.orders s
                WHERE s.id IN (SELECT id FROM matching_orders)
                GROUP BY s.source_id
                ORDER BY revenue DESC, s.source_id
            ",
            measures("op2")
        )
    } else {
        format!(
            "
                SELECT
                    {}
                FROM silver.orders s
                WHERE {wh}
                GROUP BY s.source_id
                ORDER BY revenue DESC, s.source_id
            ",
            measures("op")
        )
    }
}

fn top_products_sql(wh: &str, limit_at: &str) -> String {
    format!(
        "
                SELECT
                    COALESCE(l.catalog_product_name, l.product_name, 'Unknown') as product_name,
                    COALESCE(l.sku, '') as sku,
                    SUM(l.quantity) as quantity,
                    COALESCE(SUM(l.line_amount), 0) as revenue,
                    COUNT(DISTINCT l.order_id) as orders_count
            FROM silver.order_lines l
            WHERE {wh}
            GROUP BY COALESCE(l.catalog_product_name, l.product_name, 'Unknown'), COALESCE(l.sku, '')
            ORDER BY quantity DESC, product_name, sku
            LIMIT {limit_at}
        "
    )
}

fn by_source_sql(wh: &str) -> String {
    format!(
        "
                SELECT
                    l.source_id,
                    COALESCE(l.catalog_product_name, l.product_name, 'Unknown') as product_name,
                    SUM(l.quantity) as quantity
            FROM silver.order_lines l
            WHERE {wh}
            GROUP BY l.source_id, COALESCE(l.catalog_product_name, l.product_name, 'Unknown')
            ORDER BY l.source_id, quantity DESC, product_name
        "
    )
}

/// `marketing_period_measures(POSTGRES)`: the five scalars from the roll-up
/// rows, the channels from the per-source ones.
const PERIOD_MEASURES: &str = "COALESCE(SUM(revenue) FILTER (WHERE source_id IS NULL), 0),
        COALESCE(SUM(orders_count) FILTER (WHERE source_id IS NULL), 0),
        COALESCE(SUM(unique_customers) FILTER (WHERE source_id IS NULL), 0),
        COALESCE(SUM(new_customers) FILTER (WHERE source_id IS NULL), 0),
        COALESCE(SUM(returning_customers) FILTER (WHERE source_id IS NULL), 0),
        COALESCE(SUM(revenue) FILTER (WHERE source_id = 1), 0),
        COALESCE(SUM(revenue) FILTER (WHERE source_id = 2), 0),
        COALESCE(SUM(revenue) FILTER (WHERE source_id = 4), 0),
        COALESCE(SUM(orders_count) FILTER (WHERE source_id = 1), 0),
        COALESCE(SUM(orders_count) FILTER (WHERE source_id = 2), 0),
        COALESCE(SUM(orders_count) FILTER (WHERE source_id = 4), 0)";

/// `_MONTH_SQL`, the eleven figures from Gold.
fn gold_period_sql(sales_where: &str) -> String {
    format!(
        "
            SELECT
                {PERIOD_MEASURES}
            FROM gold.daily_revenue
            WHERE date BETWEEN $1 AND $2 AND {sales_where}
        "
    )
}

/// `_MARKETING_LINES_PERIOD_SQL` with its `{where}` filled: the same eleven
/// figures from the order lines, when a product filter narrows the report.
fn lines_period_sql(wh: &str) -> String {
    format!(
        "
    WITH cells AS (
        SELECT
            COALESCE(SUM(l.line_amount), 0) AS revenue,
            COUNT(DISTINCT l.order_id) AS orders,
            COUNT(DISTINCT l.buyer_id) AS customers,
            COUNT(DISTINCT CASE WHEN l.is_new_customer THEN l.buyer_id END) AS new_customers,
            COUNT(DISTINCT CASE WHEN NOT l.is_new_customer THEN l.buyer_id END) AS returning_customers,
            COALESCE(SUM(CASE WHEN l.source_id = 1 THEN l.line_amount END), 0) AS instagram_revenue,
            COALESCE(SUM(CASE WHEN l.source_id = 2 THEN l.line_amount END), 0) AS telegram_revenue,
            COALESCE(SUM(CASE WHEN l.source_id = 4 THEN l.line_amount END), 0) AS shopify_revenue,
            COUNT(DISTINCT CASE WHEN l.source_id = 1 THEN l.order_id END) AS instagram_orders,
            COUNT(DISTINCT CASE WHEN l.source_id = 2 THEN l.order_id END) AS telegram_orders,
            COUNT(DISTINCT CASE WHEN l.source_id = 4 THEN l.order_id END) AS shopify_orders
        FROM silver.order_lines l
        WHERE NOT l.is_return
          AND l.is_active_source
          AND l.order_date BETWEEN $1 AND $2
          AND {wh}
        GROUP BY l.order_date, l.sales_type
    )
    SELECT
        COALESCE(SUM(revenue), 0),
        COALESCE(SUM(orders), 0),
        COALESCE(SUM(customers), 0),
        COALESCE(SUM(new_customers), 0),
        COALESCE(SUM(returning_customers), 0),
        COALESCE(SUM(instagram_revenue), 0),
        COALESCE(SUM(telegram_revenue), 0),
        COALESCE(SUM(shopify_revenue), 0),
        COALESCE(SUM(instagram_orders), 0),
        COALESCE(SUM(telegram_orders), 0),
        COALESCE(SUM(shopify_orders), 0)
    FROM cells
"
    )
}

/// The brands, from the order lines at `gold_daily_products`' grain and
/// summed after.
fn brands_sql(wh: &str) -> String {
    format!(
        "
            WITH product_days AS (
                SELECT
                    l.brand,
                    SUM(l.quantity) AS quantity_sold,
                    SUM(l.line_amount) AS product_revenue,
                    COUNT(DISTINCT l.order_id) AS order_count
                FROM silver.order_lines l
                WHERE NOT l.is_return
                  AND l.is_active_source
                  AND l.order_date BETWEEN $1 AND $2
                  AND {wh}
                GROUP BY
                    l.order_date, l.sales_type, l.source_id, l.product_id,
                    l.product_name, l.brand, l.category_id, l.category_name,
                    l.parent_category_name
            )
            SELECT
                COALESCE(brand, 'Unknown') AS brand_name,
                SUM(product_revenue) AS revenue,
                SUM(order_count) AS orders,
                SUM(quantity_sold) AS quantity
            FROM product_days
            GROUP BY COALESCE(brand, 'Unknown')
            HAVING SUM(product_revenue) > 0
            ORDER BY revenue DESC, brand_name
        "
    )
}

const GOAL_SQL: &str = "SELECT goal_amount FROM app.revenue_goals WHERE period_type = 'monthly'";

// ─── Reading a row ──────────────────────────────────────────────────────────

/// An integer column whatever Postgres typed it — a COUNT is a bigint, a SUM
/// of bigints a numeric — as `int()` reads it: a numeric truncated.
fn int_col(row: &PgRow, i: usize) -> Result<Option<i64>, Failure> {
    if let Ok(v) = row.try_get::<Option<i64>, _>(i) {
        return Ok(v);
    }
    if let Ok(v) = row.try_get::<Option<i32>, _>(i) {
        return Ok(v.map(i64::from));
    }
    if let Ok(v) = row.try_get::<Option<i16>, _>(i) {
        return Ok(v.map(i64::from));
    }
    let v: Option<Decimal> = row
        .try_get(i)
        .map_err(|_| Failure::Raised("a column the statement sums is not a number"))?;
    v.map(|d| {
        d.trunc()
            .to_i64()
            .ok_or(Failure::Raised("an integer past 64 bits"))
    })
    .transpose()
}

/// `float(x)` of a numeric, a double or an integer column.
fn float_col(row: &PgRow, i: usize) -> Result<Option<f64>, Failure> {
    if let Ok(v) = row.try_get::<Option<Decimal>, _>(i) {
        return Ok(v.map(decimal_to_float));
    }
    if let Ok(v) = row.try_get::<Option<f64>, _>(i) {
        return Ok(v);
    }
    Ok(int_col(row, i)?.map(|n| n as f64))
}

/// `int(r[i] or 0)`.
fn int_or_zero(row: &PgRow, i: usize) -> Result<i64, Failure> {
    Ok(int_col(row, i)?.unwrap_or(0))
}

/// `int(r[i])`: a NULL is Python's TypeError, the app's 500.
fn int_req(row: &PgRow, i: usize) -> Result<i64, Failure> {
    int_col(row, i)?.ok_or(Failure::Raised("int(None)"))
}

/// `float(r[i] or 0)`.
fn float_or_zero(row: &PgRow, i: usize) -> Result<f64, Failure> {
    Ok(float_col(row, i)?.unwrap_or(0.0))
}

/// `float(r[i])`.
fn float_req(row: &PgRow, i: usize) -> Result<f64, Failure> {
    float_col(row, i)?.ok_or(Failure::Raised("float(None)"))
}

fn text(row: &PgRow, i: usize) -> Result<Option<String>, Failure> {
    row.try_get(i)
        .map_err(|_| Failure::Raised("a column the statement names is not text"))
}

/// `round(num / den * scale, ndigits) if den > 0 else 0`.
fn ratio(num: f64, den: f64, scale: f64, ndigits: usize) -> PyNum {
    if den > 0.0 {
        PyNum::Float(round_float(num / den * scale, ndigits))
    } else {
        PyNum::Int(0)
    }
}

// ─── The reads ──────────────────────────────────────────────────────────────

/// `_category_ids`: the category and its descendants, never empty.
async fn category_ids(store: &PgReads, category_id: i64) -> Result<Vec<i64>, Failure> {
    let rows = store
        .rows(TREE_SQL.to_owned(), &[Bind::Int(category_id)])
        .await
        .map_err(|e| Failure::Unavailable(LOOKUPS, e))?;
    let mut ids = Vec::with_capacity(rows.len());
    for row in &rows {
        ids.push(int_req(row, 0)?);
    }
    // `_at_least_the_root`: a category nobody has matches nothing, rather
    // than rendering `IN ()`.
    Ok(if ids.is_empty() {
        vec![category_id]
    } else {
        ids
    })
}

async fn tree(store: &PgReads, w: &Window) -> Result<Option<Vec<i64>>, Failure> {
    match w.category_id {
        Some(category) => Ok(Some(category_ids(store, category).await?)),
        None => Ok(None),
    }
}

#[derive(Serialize)]
struct SourceRow {
    source_id: i64,
    source_name: &'static str,
    orders_count: i64,
    products_sold: i64,
    revenue: f64,
    avg_check: PyNum,
    returns_count: i64,
    return_rate: PyNum,
}

#[derive(Serialize)]
struct Totals {
    orders_count: i64,
    products_sold: i64,
    revenue: f64,
    avg_check: PyNum,
    returns_count: i64,
    return_rate: PyNum,
}

#[derive(Serialize)]
struct Summary {
    sources: Vec<SourceRow>,
    totals: Totals,
}

/// `revenue / orders if orders > 0 else 0`, and the return rate beside it.
fn check_and_rate(revenue: f64, orders: i64, returns: i64) -> (PyNum, PyNum) {
    let avg_check = if orders > 0 {
        PyNum::Float(revenue / orders as f64)
    } else {
        PyNum::Int(0)
    };
    let return_rate = if orders + returns > 0 {
        PyNum::Float(returns as f64 / (orders + returns) as f64 * 100.0)
    } else {
        PyNum::Int(0)
    };
    (round_num(avg_check, 2), round_num(return_rate, 1))
}

/// `get_report_summary`.
async fn summary(store: &PgReads, w: &Window) -> Result<Summary, Failure> {
    let mut args = Args::default();
    let mut clauses = vec![
        format!(
            "s.order_date BETWEEN {} AND {}",
            args.next(Bind::Date(w.start)),
            args.next(Bind::Date(w.end))
        ),
        "s.is_active_source".to_owned(),
    ];
    if w.sales_type != "all" {
        let at = args.next(Bind::Text(w.sales_type.clone()));
        clauses.push(format!("s.sales_type = {at}"));
    }
    if let Some(source) = w.source_id {
        let at = args.next(Bind::Int(source));
        clauses.push(format!("s.source_id = {at}"));
    }
    let cats = tree(store, w).await?;
    clauses.extend(product_filter(
        "s",
        cats.as_deref(),
        w.brand.as_deref(),
        &mut args,
    ));
    let filtered = w.category_id.is_some() || w.brand.is_some();
    let rows = store
        .rows(summary_sql(&clauses.join(" AND "), filtered), &args.0)
        .await
        .map_err(|e| Failure::Unavailable(REPORTS, e))?;

    let mut sources = Vec::new();
    let (mut total_orders, mut total_products, mut total_returns) = (0_i64, 0_i64, 0_i64);
    let mut total_revenue = 0.0_f64;
    for row in &rows {
        let sid = int_col(row, 0)?;
        let Some(&(source_id, source_name)) = SOURCE_NAMES.iter().find(|(id, _)| Some(*id) == sid)
        else {
            continue;
        };
        let orders = int_or_zero(row, 1)?;
        let products = int_or_zero(row, 2)?;
        let revenue = float_or_zero(row, 3)?;
        let returns = int_or_zero(row, 4)?;
        let (avg_check, return_rate) = check_and_rate(revenue, orders, returns);
        sources.push(SourceRow {
            source_id,
            source_name,
            orders_count: orders,
            products_sold: products,
            revenue: round_float(revenue, 2),
            avg_check,
            returns_count: returns,
            return_rate,
        });
        total_orders += orders;
        total_products += products;
        total_revenue += revenue;
        total_returns += returns;
    }
    let (avg_check, return_rate) = check_and_rate(total_revenue, total_orders, total_returns);
    Ok(Summary {
        sources,
        totals: Totals {
            orders_count: total_orders,
            products_sold: total_products,
            revenue: round_float(total_revenue, 2),
            avg_check,
            returns_count: total_returns,
            return_rate,
        },
    })
}

#[derive(Serialize)]
struct TopRow {
    rank: i64,
    product_name: Option<String>,
    sku: Option<String>,
    quantity: i64,
    percentage: PyNum,
    revenue: f64,
    orders_count: i64,
}

/// `get_report_top_products`.
async fn top_products(store: &PgReads, w: &Window, limit: i64) -> Result<Vec<TopRow>, Failure> {
    let mut args = Args::default();
    let mut clauses = line_clauses(w, &mut args);
    if let Some(source) = w.source_id {
        let at = args.next(Bind::Int(source));
        clauses.push(format!("l.source_id = {at}"));
    }
    let cats = tree(store, w).await?;
    clauses.extend(product_filter(
        "l",
        cats.as_deref(),
        w.brand.as_deref(),
        &mut args,
    ));
    let limit_at = args.next(Bind::Int(limit));
    let rows = store
        .rows(top_products_sql(&clauses.join(" AND "), &limit_at), &args.0)
        .await
        .map_err(|e| Failure::Unavailable(REPORTS, e))?;
    let mut quantities = Vec::with_capacity(rows.len());
    for row in &rows {
        quantities.push(int_or_zero(row, 2)?);
    }
    let total: i64 = quantities.iter().sum();
    let mut out = Vec::with_capacity(rows.len());
    for ((row, quantity), rank) in rows.iter().zip(quantities).zip(1..) {
        out.push(TopRow {
            rank,
            product_name: text(row, 0)?,
            sku: text(row, 1)?,
            quantity,
            percentage: ratio(quantity as f64, total as f64, 100.0, 1),
            revenue: round_float(float_or_zero(row, 3)?, 2),
            orders_count: int_or_zero(row, 4)?,
        });
    }
    Ok(out)
}

/// `get_report_products_by_source`: by the dates and the sales type alone.
async fn products_by_source(
    store: &PgReads,
    w: &Window,
) -> Result<HashMap<i64, Vec<(String, i64)>>, Failure> {
    let mut args = Args::default();
    let clauses = line_clauses(w, &mut args);
    let rows = store
        .rows(by_source_sql(&clauses.join(" AND ")), &args.0)
        .await
        .map_err(|e| Failure::Unavailable(REPORTS, e))?;
    let mut by_source: HashMap<i64, Vec<(String, i64)>> = HashMap::new();
    for row in &rows {
        let sid = int_req(row, 0)?;
        if SOURCE_NAMES.iter().all(|(id, _)| *id != sid) {
            continue;
        }
        // `csv.writer` writes a None as an empty field.
        let name = text(row, 1)?.unwrap_or_default();
        by_source
            .entry(sid)
            .or_default()
            .push((name, int_or_zero(row, 2)?));
    }
    Ok(by_source)
}

#[derive(Serialize)]
struct Figures {
    revenue: f64,
    orders: i64,
    avg_check: PyNum,
    customers: i64,
    new_customers: i64,
    returning_customers: i64,
    return_rate: PyNum,
}

impl Figures {
    /// The seven, in the order the CSV's first section lists them.
    fn row(&self) -> [PyNum; 7] {
        [
            PyNum::Float(self.revenue),
            PyNum::Int(self.orders),
            self.avg_check,
            PyNum::Int(self.customers),
            PyNum::Int(self.new_customers),
            PyNum::Int(self.returning_customers),
            self.return_rate,
        ]
    }
}

/// One channel of the current period: revenue rounded, orders.
#[derive(Clone, Copy)]
struct Channel {
    revenue: f64,
    orders: i64,
}

#[derive(Serialize)]
struct General {
    current: Figures,
    previous: Figures,
    year_ago: Figures,
    monthly_goal: Option<f64>,
}

#[derive(Serialize)]
struct ProductFilter {
    category_id: Option<i64>,
    brand: Option<String>,
}

#[derive(Serialize)]
struct BrandRow {
    brand: Option<String>,
    revenue: f64,
    orders: i64,
    avg_check: PyNum,
    share_pct: PyNum,
}

#[derive(Serialize)]
struct SourceShare {
    source_name: &'static str,
    orders: i64,
    revenue: f64,
    orders_pct: PyNum,
    revenue_pct: PyNum,
}

#[derive(Serialize)]
struct Marketing {
    start_date: String,
    end_date: String,
    prev_start_date: String,
    prev_end_date: String,
    yoy_start_date: String,
    yoy_end_date: String,
    general_sales: General,
    product_filter: Option<ProductFilter>,
    brands: Vec<BrandRow>,
    sources: Vec<SourceShare>,
}

fn iso(d: NaiveDate) -> String {
    d.format("%Y-%m-%d").to_string()
}

/// The marketing reads' WHERE beyond the dates: the sales type, then the
/// product filter, each fragment joined with ` AND `.
fn marketing_where(w: &Window, cats: Option<&[i64]>, lines: bool, args: &mut Args) -> String {
    let mut wh = if w.sales_type == "all" {
        "1=1".to_owned()
    } else {
        let at = args.next(Bind::Text(w.sales_type.clone()));
        // Gold names the column bare, the lines through `l.`.
        if lines {
            format!("l.sales_type = {at}")
        } else {
            format!("sales_type = {at}")
        }
    };
    for fragment in product_filter("l", cats, w.brand.as_deref(), args) {
        wh.push_str(" AND ");
        wh.push_str(&fragment);
    }
    wh
}

/// `_fetch_month`: the eleven period figures, from Gold or — narrowed by a
/// product filter — from the order lines.
async fn period_figures(
    store: &PgReads,
    w: &Window,
    cats: Option<&[i64]>,
    (start, end): (NaiveDate, NaiveDate),
) -> Result<(Figures, [Channel; 3]), Failure> {
    let mut args = Args::default();
    args.next(Bind::Date(start));
    args.next(Bind::Date(end));
    let filtered = w.category_id.is_some() || w.brand.is_some();
    let sql = if filtered {
        lines_period_sql(&marketing_where(w, cats, true, &mut args))
    } else {
        gold_period_sql(&marketing_where(w, None, false, &mut args))
    };
    let rows = store
        .rows(sql, &args.0)
        .await
        .map_err(|e| Failure::Unavailable(MARKETING, e))?;
    let row = rows
        .first()
        .ok_or(Failure::Raised("an aggregate answered no row"))?;
    let revenue = float_req(row, 0)?;
    let orders = int_req(row, 1)?;
    let customers = int_req(row, 2)?;
    let returning = int_req(row, 4)?;
    let channel = |r: usize, o: usize| -> Result<Channel, Failure> {
        Ok(Channel {
            revenue: round_float(float_req(row, r)?, 2),
            orders: int_req(row, o)?,
        })
    };
    let figures = Figures {
        revenue: round_float(revenue, 2),
        orders,
        avg_check: ratio(revenue, orders as f64, 1.0, 2),
        customers,
        new_customers: int_req(row, 3)?,
        returning_customers: returning,
        return_rate: ratio(returning as f64, customers as f64, 100.0, 1),
    };
    // Instagram, Telegram, Shopify.
    Ok((figures, [channel(5, 8)?, channel(6, 9)?, channel(7, 10)?]))
}

/// `calendar.monthrange(year, month)[1]`.
fn days_in_month(d: NaiveDate) -> u32 {
    let (y, m) = if d.month() == 12 {
        (d.year() + 1, 1)
    } else {
        (d.year(), d.month() + 1)
    };
    NaiveDate::from_ymd_opt(y, m, 1)
        .and_then(|first| first.pred_opt())
        .map_or(31, |last| last.day())
}

/// `get_marketing_report_by_dates`.
async fn marketing(store: &PgReads, w: &Window) -> Result<Marketing, Failure> {
    let previous = period::comparison_window(w.start, w.end, "previous_period")?;
    let year_ago = period::comparison_window(w.start, w.end, "year_ago")?;
    let filtered = w.category_id.is_some() || w.brand.is_some();
    let cats = tree(store, w).await?;
    let cats = cats.as_deref();
    let (current, channels) = period_figures(store, w, cats, (w.start, w.end)).await?;
    let (previous_figures, _) = period_figures(store, w, cats, previous).await?;
    let (year_ago_figures, _) = period_figures(store, w, cats, year_ago).await?;

    let mut args = Args::default();
    args.next(Bind::Date(w.start));
    args.next(Bind::Date(w.end));
    let wh = marketing_where(w, cats, true, &mut args);
    let rows = store
        .rows(brands_sql(&wh), &args.0)
        .await
        .map_err(|e| Failure::Unavailable(MARKETING, e))?;
    let mut revenues = Vec::with_capacity(rows.len());
    for row in &rows {
        revenues.push(PyNum::Float(float_req(row, 1)?));
    }
    // `sum(float(r[1]) for r in …)`: CPython's compensated sum, an int 0
    // when there is nothing to add.
    let total = py_sum(&revenues).as_f64();
    let mut brands = Vec::with_capacity(rows.len());
    for (row, revenue) in rows.iter().zip(&revenues) {
        let revenue = revenue.as_f64();
        let orders = int_req(row, 2)?;
        brands.push(BrandRow {
            brand: text(row, 0)?,
            revenue: round_float(revenue, 2),
            orders,
            avg_check: ratio(revenue, orders as f64, 1.0, 2),
            share_pct: ratio(revenue, total, 100.0, 1),
        });
    }

    // Read where chain 7a writes, with no fallback: a failure there is a raw
    // exception in Python, the app's 500.
    let goals = store
        .rows(GOAL_SQL.to_owned(), &[])
        .await
        .map_err(|_| Failure::Raised("the goal read failed"))?;
    let goal = match goals.first() {
        Some(row) => Some(float_req(row, 0)?),
        None => None,
    };

    // `[("shopify", "Сайт"), ("telegram", "Telegram"), ("instagram", "Instagram")]`
    let mut sources: Vec<SourceShare> = [("Сайт", 2), ("Telegram", 1), ("Instagram", 0)]
        .into_iter()
        .map(|(source_name, i): (&'static str, usize)| {
            let channel = channels[i];
            SourceShare {
                source_name,
                orders: channel.orders,
                revenue: round_float(channel.revenue, 2),
                orders_pct: ratio(channel.orders as f64, current.orders as f64, 100.0, 1),
                revenue_pct: ratio(channel.revenue, current.revenue, 100.0, 1),
            }
        })
        .collect();
    // `sort(key=revenue, reverse=True)`: stable, and equal is Python's equal.
    sources.sort_by(|a, b| b.revenue.partial_cmp(&a.revenue).unwrap_or(Ordering::Equal));

    let full_month = w.start.day() == 1
        && w.end.day() == days_in_month(w.end)
        && w.start.month() == w.end.month()
        && w.start.year() == w.end.year();
    Ok(Marketing {
        start_date: iso(w.start),
        end_date: iso(w.end),
        prev_start_date: iso(previous.0),
        prev_end_date: iso(previous.1),
        yoy_start_date: iso(year_ago.0),
        yoy_end_date: iso(year_ago.1),
        general_sales: General {
            current,
            previous: previous_figures,
            year_ago: year_ago_figures,
            // The goal is the whole shop's: beside one brand it would read
            // as that brand's target.
            monthly_goal: goal.filter(|_| full_month && !filtered),
        },
        product_filter: filtered.then(|| ProductFilter {
            category_id: w.category_id,
            brand: w.brand.clone(),
        }),
        brands,
        sources,
    })
}

// ─── Python's text, for the CSVs ────────────────────────────────────────────

/// `format(x, '.Nf')`: the exact binary value rounded half to even, which
/// Rust's fixed precision does too.
fn fixed(x: f64, n: usize) -> String {
    format!("{x:.n$}")
}

/// The `,` option: groups of three in the integer part.
fn group(text: &str) -> String {
    let (sign, rest) = text
        .strip_prefix('-')
        .map_or(("", text), |rest| ("-", rest));
    let (whole, fraction) = rest
        .split_once('.')
        .map_or((rest, None), |(w, f)| (w, Some(f)));
    let mut grouped = String::with_capacity(whole.len() + whole.len() / 3);
    for (i, c) in whole.chars().enumerate() {
        if i > 0 && (whole.len() - i) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(c);
    }
    match fraction {
        Some(fraction) => format!("{sign}{grouped}.{fraction}"),
        None => format!("{sign}{grouped}"),
    }
}

/// `_fmt_currency`: `f"₴{v:,.0f}"`.
fn currency(v: f64) -> String {
    format!("₴{}", group(&fixed(v, 0)))
}

fn money(v: PyNum) -> String {
    currency(v.as_f64())
}

/// `f"{v:,}"`.
fn grouped(v: PyNum) -> String {
    match v {
        PyNum::Int(n) => group(&n.to_string()),
        PyNum::Float(x) => group(&repr_float(x)),
    }
}

/// `f"{v:.0f}%"`.
fn whole_percent(v: PyNum) -> String {
    format!("{}%", fixed(v.as_f64(), 0))
}

/// `repr(float)`: the shortest digits that read back, positional from 1e-4
/// up to 1e16 and scientific outside.
fn repr_float(x: f64) -> String {
    if x == 0.0 {
        return if x.is_sign_negative() { "-0.0" } else { "0.0" }.to_owned();
    }
    let sci = format!("{x:e}");
    let (mantissa, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let (sign, mantissa) = mantissa
        .strip_prefix('-')
        .map_or(("", mantissa), |m| ("-", m));
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    let body = if (-4..16).contains(&exp) {
        let point = exp + 1;
        if point <= 0 {
            format!("0.{}{digits}", "0".repeat(point.unsigned_abs() as usize))
        } else if point as usize >= digits.len() {
            format!("{digits}{}.0", "0".repeat(point as usize - digits.len()))
        } else {
            let (whole, fraction) = digits.split_at(point as usize);
            format!("{whole}.{fraction}")
        }
    } else {
        let head = if digits.len() > 1 {
            format!("{}.{}", &digits[..1], &digits[1..])
        } else {
            digits.clone()
        };
        let sign_e = if exp < 0 { '-' } else { '+' };
        format!("{head}e{sign_e}{:02}", exp.unsigned_abs())
    };
    format!("{sign}{body}")
}

/// `str(v)` of an int-or-float.
fn py_str(v: PyNum) -> String {
    match v {
        PyNum::Int(n) => n.to_string(),
        PyNum::Float(x) => repr_float(x),
    }
}

/// `_fmt_pct_change(cur, prev)`.
fn pct_change(cur: PyNum, prev: PyNum) -> String {
    let (cur, prev) = (cur.as_f64(), prev.as_f64());
    if prev == 0.0 {
        return if cur > 0.0 { "+100%" } else { "0%" }.to_owned();
    }
    format!("{}%", fixed((cur - prev) / prev * 100.0, 2))
}

/// `_fmt_goal(v)` of a goal that is there.
fn goal_text(v: f64) -> String {
    if v >= 1_000_000.0 {
        let m = v / 1_000_000.0;
        // `m % 1` of a positive float is its fraction.
        let digits = if m % 1.0 != 0.0 { 1 } else { 0 };
        return format!("₴{} млн", fixed(m, digits));
    }
    currency(v)
}

/// `csv.writer` with the excel dialect: `,` between fields and CRLF after
/// each row; a field quoted only when it holds `,` `"` `\r` or `\n`, with a
/// `"` doubled; a row of one empty field written `""`, so it is not mistaken
/// for an empty row. `encode("utf-8-sig")` puts the BOM first.
#[derive(Default)]
struct Csv(String);

impl Csv {
    fn row<S: AsRef<str>>(&mut self, fields: &[S]) {
        for (i, field) in fields.iter().enumerate() {
            let field = field.as_ref();
            if i > 0 {
                self.0.push(',');
            }
            if field.contains([',', '"', '\r', '\n']) || (fields.len() == 1 && field.is_empty()) {
                self.0.push('"');
                self.0.push_str(&field.replace('"', "\"\""));
                self.0.push('"');
            } else {
                self.0.push_str(field);
            }
        }
        self.0.push_str("\r\n");
    }

    fn bytes(self) -> Vec<u8> {
        let mut out = "\u{feff}".as_bytes().to_vec();
        out.extend(self.0.into_bytes());
        out
    }
}

const NO_FIELDS: [&str; 0] = [];

/// The `type=summary` export: the totals, then each source with its
/// products, as the bot's Excel lays them out.
fn summary_csv(
    w: &Window,
    data: &Summary,
    by_source: &HashMap<i64, Vec<(String, i64)>>,
) -> Vec<u8> {
    let mut csv = Csv::default();
    let shown = if w.start == w.end {
        iso(w.start)
    } else {
        format!("{} to {}", iso(w.start), iso(w.end))
    };
    csv.row(&[format!("Sales Summary for {shown} (Timezone: Europe/Kyiv)")]);
    csv.row(&[format!("Total Orders: {}", data.totals.orders_count)]);
    csv.row(&NO_FIELDS);
    for source in &data.sources {
        csv.row(&[format!("Source: {}", source.source_name)]);
        csv.row(&[format!("Total Orders: {}", source.orders_count)]);
        csv.row(&[format!(
            "Average Check: {} UAH",
            fixed(source.avg_check.as_f64(), 2)
        )]);
        csv.row(&[format!(
            "Returns: {} ({}%)",
            source.returns_count,
            py_str(source.return_rate)
        )]);
        csv.row(&["Product", "Quantity"]);
        for (name, quantity) in by_source.get(&source.source_id).into_iter().flatten() {
            csv.row(&[name.clone(), quantity.to_string()]);
        }
        csv.row(&NO_FIELDS);
    }
    csv.bytes()
}

/// The `type=top_products` export: each value as `str()` writes it.
fn top_products_csv(products: &[TopRow]) -> Vec<u8> {
    let mut csv = Csv::default();
    csv.row(&["#", "Product", "SKU", "Qty", "%", "Revenue", "Orders"]);
    for p in products {
        csv.row(&[
            p.rank.to_string(),
            p.product_name.clone().unwrap_or_default(),
            p.sku.clone().unwrap_or_default(),
            p.quantity.to_string(),
            py_str(p.percentage),
            repr_float(p.revenue),
            p.orders_count.to_string(),
        ]);
    }
    csv.bytes()
}

/// How a figure is written in the first section.
type Format = fn(PyNum) -> String;

/// The labels and formats of the first section, in its order.
const GENERAL: [(&str, Format); 7] = [
    ("Виручка (грн)", money),
    ("К-сть замовлень", grouped),
    ("Середній чек (грн)", money),
    ("К-сть клієнтів", grouped),
    ("Нові клієнти", grouped),
    ("Клієнти, що повернулись", grouped),
    ("% повернення", whole_percent),
];

/// `export_marketing_csv`: the three sections the page shows.
fn marketing_csv(w: &Window, report: &Marketing) -> Vec<u8> {
    let range = |a: &str, b: &str| {
        if a == b {
            a.to_owned()
        } else {
            format!("{a} — {b}")
        }
    };
    let current = range(&report.start_date, &report.end_date);
    let previous = range(&report.prev_start_date, &report.prev_end_date);
    let year_ago = range(&report.yoy_start_date, &report.yoy_end_date);
    let mut csv = Csv::default();
    csv.row(&[format!("Маркетинговий звіт: {current}")]);
    if report.product_filter.is_some() {
        let mut narrowed = Vec::new();
        if let Some(brand) = &w.brand {
            narrowed.push(format!("бренд {brand}"));
        }
        if let Some(category) = w.category_id {
            narrowed.push(format!("категорія #{category}"));
        }
        csv.row(&[format!(
            "Лише {}: виручка — сума позицій",
            narrowed.join(", ")
        )]);
    }
    csv.row(&NO_FIELDS);

    csv.row(&["1. ЗАГАЛЬНІ ПРОДАЖІ"]);
    let g = &report.general_sales;
    let mut header = vec![
        "Показник".to_owned(),
        current,
        previous,
        "Зміна, %".to_owned(),
        year_ago,
        "Зміна YoY".to_owned(),
    ];
    if g.monthly_goal.is_some() {
        header.push("Ціль місяця".to_owned());
    }
    csv.row(&header);
    let (cur, prev, yoy) = (g.current.row(), g.previous.row(), g.year_ago.row());
    for (i, (label, format)) in GENERAL.iter().enumerate() {
        let mut row = vec![
            (*label).to_owned(),
            format(cur[i]),
            format(prev[i]),
            pct_change(cur[i], prev[i]),
            format(yoy[i]),
            pct_change(cur[i], yoy[i]),
        ];
        if let Some(goal) = g.monthly_goal {
            // Only the revenue row carries the target.
            row.push(if i == 0 {
                goal_text(goal)
            } else {
                String::new()
            });
        }
        csv.row(&row);
    }
    csv.row(&NO_FIELDS);

    csv.row(&["2. ПРОДАЖІ ПО БРЕНДАХ"]);
    csv.row(&[
        "Бренд",
        "Виручка (грн)",
        "К-сть замовлень",
        "Ср. чек (грн)",
        "% від загального",
    ]);
    for b in &report.brands {
        csv.row(&[
            b.brand.clone().unwrap_or_default(),
            currency(b.revenue),
            grouped(PyNum::Int(b.orders)),
            money(b.avg_check),
            format!("{}%", py_str(b.share_pct)),
        ]);
    }
    csv.row(&NO_FIELDS);

    csv.row(&["3. КАНАЛИ / ДЖЕРЕЛА"]);
    csv.row(&[
        "Канал",
        "К-сть замовлень",
        "Виручка (грн)",
        "% замовлень",
        "% виручки",
    ]);
    for s in &report.sources {
        csv.row(&[
            s.source_name.to_owned(),
            grouped(PyNum::Int(s.orders)),
            currency(s.revenue),
            format!("{}%", py_str(s.orders_pct)),
            format!("{}%", py_str(s.revenue_pct)),
        ]);
    }
    csv.bytes()
}

fn json<T: Serialize>(value: &T) -> Answer {
    Answer::Json(serde_json::to_vec(value).expect("the body is plain data"))
}

/// The body for a 200, or the reason there is none. `admit` is the route's
/// rate limit, which slowapi checks once the parameters parse and before the
/// route's own checks run.
pub async fn answer(
    route: Route,
    store: &PgReads,
    raw_query: &str,
    now: DateTime<Utc>,
    admit: impl FnOnce() -> Result<(), Failure>,
) -> Result<Answer, Failure> {
    let q = parse(route, raw_query)?;
    admit()?;
    match route {
        Route::Summary => {
            let w = reports_window(&q, now)?;
            Ok(json(&summary(store, &w).await?))
        }
        Route::TopProducts => {
            let w = reports_window(&q, now)?;
            let limit = validate_limit(q.limit, 50)?;
            Ok(json(&top_products(store, &w, limit).await?))
        }
        Route::AllProducts => {
            let w = reports_window(&q, now)?;
            Ok(json(&top_products(store, &w, 5000).await?))
        }
        Route::ExportCsv => {
            // Before the common parameters are looked at.
            let kind = q.kind.as_deref().unwrap_or_default();
            if kind != "summary" && kind != "top_products" {
                return Err(Failure::BadRequest(
                    "type must be 'summary' or 'top_products'".to_owned(),
                ));
            }
            let w = reports_window(&q, now)?;
            let (start, end) = (iso(w.start), iso(w.end));
            if kind == "summary" {
                let data = summary(store, &w).await?;
                let by_source = products_by_source(store, &w).await?;
                Ok(Answer::Csv {
                    body: summary_csv(&w, &data, &by_source),
                    filename: format!("sales_report_{start}_{end}.csv"),
                })
            } else {
                let limit = validate_limit(q.limit, 5000)?;
                let products = top_products(store, &w, limit).await?;
                Ok(Answer::Csv {
                    body: top_products_csv(&products),
                    filename: format!("top_products_{start}_{end}.csv"),
                })
            }
        }
        Route::Marketing => {
            let w = marketing_window(&q, now)?;
            Ok(json(&marketing(store, &w).await?))
        }
        Route::MarketingCsv => {
            let w = marketing_window(&q, now)?;
            let report = marketing(store, &w).await?;
            Ok(Answer::Csv {
                body: marketing_csv(&w, &report),
                filename: format!(
                    "marketing_report_{}_{}.csv",
                    report.start_date, report.end_date
                ),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_formats_the_numbers_this_way() {
        assert_eq!(currency(1_234_567.5), "₴1,234,568");
        assert_eq!(currency(2.5), "₴2");
        assert_eq!(currency(0.0), "₴0");
        assert_eq!(currency(-1234.4), "₴-1,234");
        assert_eq!(grouped(PyNum::Int(1_000)), "1,000");
        assert_eq!(grouped(PyNum::Int(999)), "999");
        assert_eq!(fixed(0.125, 2), "0.12");
        assert_eq!(fixed(0.375, 2), "0.38");
        assert_eq!(whole_percent(PyNum::Float(12.5)), "12%");
        assert_eq!(whole_percent(PyNum::Int(0)), "0%");
        assert_eq!(pct_change(PyNum::Int(5), PyNum::Int(0)), "+100%");
        assert_eq!(pct_change(PyNum::Int(0), PyNum::Float(0.0)), "0%");
        assert_eq!(pct_change(PyNum::Float(150.0), PyNum::Int(100)), "50.00%");
        assert_eq!(goal_text(4_000_000.0), "₴4 млн");
        assert_eq!(goal_text(4_200_000.0), "₴4.2 млн");
        assert_eq!(goal_text(999_999.0), "₴999,999");
    }

    #[test]
    fn repr_is_pythons() {
        assert_eq!(repr_float(100.0), "100.0");
        assert_eq!(repr_float(12.3), "12.3");
        assert_eq!(repr_float(0.0001), "0.0001");
        assert_eq!(repr_float(0.00001), "1e-05");
        assert_eq!(repr_float(0.000_012_5), "1.25e-05");
        assert_eq!(repr_float(1e16), "1e+16");
        assert_eq!(repr_float(1_234_567_890_123_456.0), "1234567890123456.0");
        assert_eq!(repr_float(-0.5), "-0.5");
        assert_eq!(py_str(PyNum::Int(0)), "0");
    }

    #[test]
    fn the_csv_is_excels() {
        let mut csv = Csv::default();
        csv.row(&[""]);
        csv.row(&NO_FIELDS);
        csv.row(&["a,b", "say \"x\"", ""]);
        csv.row(&["line\nbreak", "plain"]);
        assert_eq!(
            csv.bytes(),
            "\u{feff}\"\"\r\n\r\n\"a,b\",\"say \"\"x\"\"\",\r\n\"line\nbreak\",plain\r\n"
                .as_bytes()
        );
    }

    #[test]
    fn brand_where_names_the_unknown_bucket() {
        let mut args = Args::default();
        assert_eq!(
            brand_where(" Unknown ", &mut args, "l"),
            "(l.brand IS NULL OR TRIM(l.brand) = '')"
        );
        assert!(args.0.is_empty());
        assert_eq!(
            brand_where("COSRX", &mut args, "s"),
            "LOWER(s.brand) = LOWER($1)"
        );
    }

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
    fn the_integers_are_pydantics_up_to_64_bits() {
        assert_eq!(
            kinds(Route::ExportCsv, "limit=x&source_id=y"),
            [
                ("missing", "type"),
                ("int_parsing", "source_id"),
                ("int_parsing", "limit")
            ]
        );
        assert!(
            kinds(
                Route::TopProducts,
                "limit=%205&source_id=1_0&category_id=2.0"
            )
            .is_empty()
        );
        assert_eq!(
            kinds(Route::Summary, "category_id=99999999999999999999"),
            [("int_parsing", "category_id")]
        );
        // What a route does not declare, FastAPI never reads.
        assert!(kinds(Route::Marketing, "source_id=x&limit=y&type=z").is_empty());
        assert!(kinds(Route::AllProducts, "limit=y").is_empty());
    }

    #[test]
    fn a_missing_type_is_fastapis_422() {
        let Err(Failure::Unprocessable(errors)) = parse(Route::ExportCsv, "") else {
            panic!("expected a 422");
        };
        assert_eq!(
            serde_json::to_string(&errors).unwrap(),
            r#"[{"type":"missing","loc":["query","type"],"msg":"Field required","input":null}]"#
        );
    }
}
