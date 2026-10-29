//! The analytics router's reads but the trend and the forecast, answered as
//! `web/routes/api/analytics.py` answers them with `KS_READ_LOOKUPS`,
//! `KS_READ_GOLD`, `KS_READ_SILVER`, `KS_READ_DASHBOARD` and
//! `KS_READ_MARKETING` at `postgres`, as production has them:
//!
//! - the filter bar's lookups: `/api/categories`, a root's children,
//!   `/api/brands`, `/api/promocodes`;
//! - the dashboard's cards and charts: `/api/summary`, `/sales/by-source`,
//!   `/products/top`, `/products/performance`, `/categories/breakdown`,
//!   `/brands/analytics`, and the returns list, `/api/returns`;
//! - /marketing's `/api/promocodes/analytics`.
//!
//! The statements are `core/repositories/revenue.py`'s — the summary's Gold
//! is `core/pg_gold_read.fetch_summary`'s — as Postgres gets them, and the
//! Python over the rows is the repository's: `round()` to even, `sum()`
//! compensated, `sorted()` stable, an `int` 0 where Python never made a
//! float.
//!
//! Not here: `/api/revenue/forecast` and `/forecast/evaluate`, whose model is
//! the Python process's, and the trend's forecast overlay, which `trend.rs`
//! answers 501.

use std::cmp::Ordering;
use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use sqlx::Row;
use sqlx::postgres::PgRow;

use crate::params::{FieldError, PyInt, int_message, lax_int};
use crate::period;
use crate::pgread::{Bind, PgReads};
use crate::pyrt::{PyNum, py_isspace, py_strip, py_sum, round_float, round_num};
use crate::reports::{self, Args};
use crate::trend::Failure;
use crate::validate;

/// The surfaces `read_fallback` names in a 503.
const LOOKUPS: &str = "lookups";
const DASHBOARD: &str = "dashboard";
const MARKETING: &str = "marketing";

/// `_GOLD_SOURCE_COLUMNS`: the sources Gold had a column for.
const GOLD_SOURCES: [i64; 3] = [1, 2, 4];
/// `source_names` and `source_colors` in `get_sales_by_source`.
const SOURCES: [(i64, &str, &str); 3] = [
    (1, "Instagram", "#7C3AED"),
    (2, "Telegram", "#2563EB"),
    (4, "Shopify", "#eb4200"),
];
/// `category_colors`.
const CATEGORY_COLORS: [&str; 8] = [
    "#7C3AED", "#2563EB", "#16A34A", "#F59E0B", "#eb4200", "#EC4899", "#8B5CF6", "#06B6D4",
];
/// `brand_colors` and `promo_colors`: the same eight, then two more.
const TEN_COLORS: [&str; 10] = [
    "#7C3AED", "#2563EB", "#16A34A", "#F59E0B", "#eb4200", "#EC4899", "#8B5CF6", "#06B6D4",
    "#14B8A6", "#EF4444",
];
/// `STATUS_NAMES` in `get_return_orders`: `OrderStatus.return_statuses()`,
/// each member's name with `_` as a space, `.title()`d.
const STATUS_NAMES: [(i64, &str); 6] = [
    (15, "Not Available"),
    (18, "Did Not Arrange Price"),
    (19, "Canceled"),
    (21, "Delivery Failed"),
    (22, "Returned"),
    (23, "Returning"),
];
/// `UNKNOWN_BRAND` (`core/duckdb_constants.py`).
const UNKNOWN_BRAND: &str = "Unknown";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    Categories,
    ChildCategories,
    Brands,
    Promocodes,
    SalesBySource,
    TopProducts,
    Summary,
    Returns,
    ProductPerformance,
    BrandAnalytics,
    PromocodeAnalytics,
    CategoryBreakdown,
}

impl Route {
    pub fn from_endpoint(name: &str) -> Option<Self> {
        Some(match name.strip_prefix("web.routes.api.analytics.")? {
            "get_categories" => Route::Categories,
            "get_child_categories" => Route::ChildCategories,
            "get_brands" => Route::Brands,
            "get_promocodes" => Route::Promocodes,
            "get_sales_by_source" => Route::SalesBySource,
            "get_top_products" => Route::TopProducts,
            "get_summary" => Route::Summary,
            "get_returns" => Route::Returns,
            "get_product_performance" => Route::ProductPerformance,
            "get_brand_analytics" => Route::BrandAnalytics,
            "get_promocode_analytics" => Route::PromocodeAnalytics,
            "get_subcategory_breakdown" => Route::CategoryBreakdown,
            _ => return None,
        })
    }

    /// The signature names `source_id`, `brand` and `promocode`.
    fn takes_line_filters(self) -> bool {
        matches!(
            self,
            Route::SalesBySource
                | Route::TopProducts
                | Route::Summary
                | Route::ProductPerformance
                | Route::CategoryBreakdown
        )
    }

    /// The signature names `category_id`.
    fn takes_category(self) -> bool {
        matches!(
            self,
            Route::SalesBySource | Route::TopProducts | Route::Summary
        )
    }
}

// ─── The query string ───────────────────────────────────────────────────────

/// The route's parameters after FastAPI, before its own checks.
struct Query {
    parent_category: Option<String>,
    period: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
    /// Kept past 64 bits: `validate_source_id` refuses it with its value.
    source_id: Option<PyInt>,
    category_id: Option<PyInt>,
    brand: Option<String>,
    promocode: Option<String>,
    /// `/products/top`'s `limit: int = Query(10)`: no bounds, so any int
    /// reaches `validate_limit`.
    top_limit: PyInt,
    /// `/returns`' `limit: int = Query(50, ge=1, le=100)`.
    returns_limit: i64,
    sales_type: String,
}

fn field_error(
    kind: &'static str,
    at: &'static str,
    name: &'static str,
    msg: &'static str,
    input: Option<String>,
) -> FieldError {
    FieldError {
        kind,
        loc: (at, name),
        msg,
        input,
    }
}

/// pydantic's lax `int`, past 64 bits the digits Python's int would hold.
fn lax(
    raw: String,
    at: &'static str,
    name: &'static str,
    errors: &mut Vec<FieldError>,
) -> Option<PyInt> {
    match lax_int(&raw) {
        Ok(v) => Some(v),
        Err(kind) => {
            errors.push(field_error(kind, at, name, int_message(kind), Some(raw)));
            None
        }
    }
}

/// `limit: int = Query(50, ge=1, le=100)`; past 64 bits pydantic holds the
/// number to the bound its sign breaks.
fn returns_limit(raw: Option<String>, errors: &mut Vec<FieldError>) -> i64 {
    let Some(raw) = raw else { return 50 };
    let Some(value) = lax(raw.clone(), "query", "limit", errors) else {
        return 50;
    };
    let below = match &value {
        PyInt::Small(v) if (1..=100).contains(v) => return *v,
        PyInt::Small(v) => *v < 1,
        PyInt::Big(digits) => digits.starts_with('-'),
    };
    errors.push(if below {
        field_error(
            "greater_than_equal",
            "query",
            "limit",
            "Input should be greater than or equal to 1",
            Some(raw),
        )
    } else {
        field_error(
            "less_than_equal",
            "query",
            "limit",
            "Input should be less than or equal to 100",
            Some(raw),
        )
    });
    50
}

fn parse(route: Route, raw: &str) -> Result<Query, Failure> {
    let mut last = HashMap::new();
    for (name, value) in form_urlencoded::parse(raw.as_bytes()) {
        last.insert(name.into_owned(), value.into_owned());
    }
    let mut take = |name: &str| last.remove(name);
    let mut errors = Vec::new();
    // FastAPI reads each signature in its order: `parent_category` first on
    // the breakdown, `limit` before `sales_type` on the top products and
    // after it on the returns.
    let mut parent_category = None;
    if route == Route::CategoryBreakdown {
        parent_category = take("parent_category");
        if parent_category.is_none() {
            errors.push(field_error(
                "missing",
                "query",
                "parent_category",
                "Field required",
                None,
            ));
        }
    }
    let period = take("period");
    let start_date = take("start_date");
    let end_date = take("end_date");
    let source_id = match take("source_id") {
        Some(raw) if route.takes_line_filters() => lax(raw, "query", "source_id", &mut errors),
        _ => None,
    };
    let category_id = match take("category_id") {
        Some(raw) if route.takes_category() => lax(raw, "query", "category_id", &mut errors),
        _ => None,
    };
    let (brand, promocode) = if route.takes_line_filters() {
        (take("brand"), take("promocode"))
    } else {
        (None, None)
    };
    let mut top_limit = PyInt::Small(10);
    if route == Route::TopProducts
        && let Some(raw) = take("limit")
        && let Some(value) = lax(raw, "query", "limit", &mut errors)
    {
        top_limit = value;
    }
    let sales_type = take("sales_type").unwrap_or_else(|| "retail".to_owned());
    let returns_limit = if route == Route::Returns {
        returns_limit(take("limit"), &mut errors)
    } else {
        50
    };
    if !errors.is_empty() {
        return Err(Failure::Unprocessable(errors));
    }
    Ok(Query {
        parent_category,
        period,
        start_date,
        end_date,
        source_id,
        category_id,
        brand,
        promocode,
        top_limit,
        returns_limit,
        sales_type,
    })
}

/// What the route goes on with once its checks pass.
struct Asked {
    start: NaiveDate,
    end: NaiveDate,
    source_id: Option<i64>,
    category_id: Option<i64>,
    brand: Option<String>,
    /// Stripped, and `None` when that left nothing: `if promocode:`.
    promocode: Option<String>,
    top_limit: i64,
    sales_type: String,
}

impl Asked {
    /// `category_id or brand or promocode`: a filter Gold has folded away.
    fn filtered(&self) -> bool {
        self.category_id.is_some() || self.brand.is_some() || self.promocode.is_some()
    }
}

/// `validate_limit(limit, max_value=50)`, which sees the int pydantic made.
fn validate_top_limit(value: &PyInt) -> Result<i64, Failure> {
    match value {
        PyInt::Small(v) if (1..=50).contains(v) => Ok(*v),
        v if !v.is_positive() => Err(Failure::BadRequest(format!(
            "limit: Must be at least 1 (got: {})",
            v.repr()
        ))),
        v => Err(Failure::BadRequest(format!(
            "limit: Cannot exceed 50 (got: {})",
            v.repr()
        ))),
    }
}

/// The route's checks, in its order, then `parse_period`.
fn asked(route: Route, q: &Query, today: NaiveDate) -> Result<Asked, Failure> {
    let bad = Failure::BadRequest;
    validate::period(q.period.as_deref()).map_err(bad)?;
    let source_id = if route.takes_line_filters() {
        validate::source_id(q.source_id.as_ref())
            .map_err(bad)?
            .map(i64::from)
    } else {
        None
    };
    let mut category_id = None;
    if route.takes_category()
        && validate::category_id(q.category_id.as_ref()).map_err(bad)?
        && let Some(PyInt::Small(id)) = q.category_id
    {
        category_id = Some(id);
    }
    let (mut brand, mut promocode) = (None, None);
    if route.takes_line_filters() {
        brand = validate::brand_value(q.brand.as_deref()).map_err(bad)?;
        if validate::promocode(q.promocode.as_deref()).map_err(bad)? {
            promocode = Some(py_strip(q.promocode.as_deref().unwrap_or_default()).to_owned());
        }
    }
    let top_limit = if route == Route::TopProducts {
        validate_top_limit(&q.top_limit)?
    } else {
        10
    };
    let sales_type = validate::sales_type(&q.sales_type).map_err(bad)?;
    let (start, end) = period::parse_period(
        q.period.as_deref(),
        q.start_date.as_deref(),
        q.end_date.as_deref(),
        today,
    )?;
    Ok(Asked {
        start,
        end,
        source_id,
        category_id,
        brand,
        promocode,
        top_limit,
        sales_type,
    })
}

// ─── Reading ────────────────────────────────────────────────────────────────

async fn read(
    store: &PgReads,
    surface: &'static str,
    sql: String,
    binds: &[Bind],
) -> Result<Vec<PgRow>, Failure> {
    store
        .rows(sql, binds)
        .await
        .map_err(|e| Failure::Unavailable(surface, e))
}

fn json<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).expect("the body is plain data")
}

/// `float(x)` of a column Python never finds NULL.
fn float_req(row: &PgRow, i: usize) -> Result<f64, Failure> {
    reports::float_col(row, i)?.ok_or(Failure::Raised("float(None)"))
}

fn text_req(row: &PgRow, i: usize) -> Result<String, Failure> {
    reports::text(row, i)?.ok_or(Failure::Raised("a name column is NULL"))
}

/// The one aggregate row a statement without `GROUP BY` returns.
fn single(rows: &[PgRow]) -> Result<&PgRow, Failure> {
    rows.first()
        .ok_or(Failure::Raised("an aggregate returned no row"))
}

/// `l.order_date BETWEEN ? AND ?`, `NOT l.is_return`, `l.is_active_source`
/// and the sales type: how every line-level read here opens.
fn line_window(a: &Asked, args: &mut Args) -> Vec<String> {
    let mut clauses = vec![
        format!(
            "l.order_date BETWEEN {} AND {}",
            args.next(Bind::Date(a.start)),
            args.next(Bind::Date(a.end))
        ),
        "NOT l.is_return".to_owned(),
        "l.is_active_source".to_owned(),
    ];
    if a.sales_type != "all" {
        let at = args.next(Bind::Text(a.sales_type.clone()));
        clauses.push(format!("l.sales_type = {at}"));
    }
    clauses
}

fn source_clause(a: &Asked, alias: &str, args: &mut Args) -> Option<String> {
    a.source_id.map(|source| {
        let at = args.next(Bind::Int(source));
        format!("{alias}.source_id = {at}")
    })
}

fn category_clause(cats: Option<&[i64]>, args: &mut Args) -> Option<String> {
    cats.map(|cats| {
        let at: Vec<String> = cats.iter().map(|c| args.next(Bind::Int(*c))).collect();
        format!("l.category_id IN ({})", at.join(","))
    })
}

fn promocode_clause(promocode: Option<&str>, args: &mut Args) -> Option<String> {
    promocode.map(|code| {
        let at = args.next(Bind::Text(code.to_owned()));
        format!("UPPER(l.promocode) = UPPER({at})")
    })
}

/// The line-level filter the dashboard's product reads share: the window,
/// then source, the category tree, brand, promocode — the order Python
/// appends their values in.
fn line_where(a: &Asked, source: bool, cats: Option<&[i64]>, args: &mut Args) -> String {
    let mut clauses = line_window(a, args);
    if source {
        clauses.extend(source_clause(a, "l", args));
    }
    clauses.extend(category_clause(cats, args));
    if let Some(brand) = &a.brand {
        clauses.push(reports::brand_where(brand, args, "l"));
    }
    clauses.extend(promocode_clause(a.promocode.as_deref(), args));
    clauses.join(" AND ")
}

/// `_category_ids` when a category was asked for: the lookups' surface,
/// before anything else is read.
async fn tree(store: &PgReads, a: &Asked) -> Result<Option<Vec<i64>>, Failure> {
    match a.category_id {
        Some(category) => Ok(Some(reports::category_ids(store, category).await?)),
        None => Ok(None),
    }
}

// ─── The filter bar ─────────────────────────────────────────────────────────

#[derive(Serialize)]
struct Category {
    id: i64,
    name: String,
}

#[derive(Serialize)]
struct Named {
    name: String,
}

async fn categories(store: &PgReads, parent: Option<i64>) -> Result<Vec<u8>, Failure> {
    let (sql, binds) = match parent {
        None => (
            "SELECT id, name FROM bronze.categories WHERE parent_id IS NULL ORDER BY name",
            vec![],
        ),
        Some(id) => (
            "SELECT id, name FROM bronze.categories WHERE parent_id = $1 ORDER BY name",
            vec![Bind::Int(id)],
        ),
    };
    let rows = read(store, LOOKUPS, sql.to_owned(), &binds).await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        out.push(Category {
            id: reports::int_req(row, 0)?,
            name: text_req(row, 1)?,
        });
    }
    Ok(json(&out))
}

/// Every brand, then the unknown bucket when something is in it.
async fn brands(store: &PgReads) -> Result<Vec<u8>, Failure> {
    let rows = read(
        store,
        LOOKUPS,
        "SELECT DISTINCT brand FROM bronze.products \
         WHERE brand IS NOT NULL AND brand != '' ORDER BY brand"
            .to_owned(),
        &[],
    )
    .await?;
    let mut out = Vec::with_capacity(rows.len() + 1);
    for row in &rows {
        out.push(Named {
            name: text_req(row, 0)?,
        });
    }
    let unbranded = read(
        store,
        LOOKUPS,
        "SELECT EXISTS (SELECT 1 FROM bronze.products WHERE brand IS NULL OR TRIM(brand) = '')"
            .to_owned(),
        &[],
    )
    .await?;
    let found: Option<bool> = match unbranded.first() {
        Some(row) => row
            .try_get(0)
            .map_err(|_| Failure::Raised("EXISTS is not a boolean"))?,
        None => None,
    };
    if found == Some(true) {
        out.push(Named {
            name: UNKNOWN_BRAND.to_owned(),
        });
    }
    Ok(json(&out))
}

async fn promocodes(store: &PgReads) -> Result<Vec<u8>, Failure> {
    let rows = read(
        store,
        LOOKUPS,
        "SELECT DISTINCT promocode FROM silver.orders \
         WHERE promocode IS NOT NULL AND promocode != '' ORDER BY promocode"
            .to_owned(),
        &[],
    )
    .await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        out.push(Named {
            name: text_req(row, 0)?,
        });
    }
    Ok(json(&out))
}

// ─── /api/summary ───────────────────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Summary {
    total_orders: i64,
    total_revenue: f64,
    avg_check: PyNum,
    total_returns: i64,
    returns_revenue: f64,
    start_date: String,
    end_date: String,
}

/// `(orders, revenue, returns, returns_revenue)` as `get_summary_stats`
/// gets them: Gold without a product filter, the order lines with one.
async fn summary_totals(store: &PgReads, a: &Asked) -> Result<(i64, f64, i64, f64), Failure> {
    if !a.filtered() {
        if let Some(source) = a.source_id
            && !GOLD_SOURCES.contains(&source)
        {
            // Opencart has no column in Gold: Postgres is not asked, and
            // under KS_DUCKDB=off the route answers DuckDB's four zeros.
            return Ok((0, 0.0, 0, 0.0));
        }
        // `pg_gold_read.fetch_summary`, which asks no revision.
        let mut binds = vec![Bind::Date(a.start), Bind::Date(a.end)];
        let mut clauses = Vec::new();
        match a.source_id {
            None => clauses.push("source_id IS NULL".to_owned()),
            Some(source) => {
                binds.push(Bind::Int(source));
                clauses.push(format!("source_id = ${}", binds.len()));
            }
        }
        if a.sales_type != "all" {
            binds.push(Bind::Text(a.sales_type.clone()));
            clauses.push(format!("sales_type = ${}", binds.len()));
        }
        let sql = format!(
            "SELECT COALESCE(SUM(orders_count), 0), COALESCE(SUM(revenue), 0), \
             COALESCE(SUM(returns_count), 0), COALESCE(SUM(returns_revenue), 0) \
             FROM gold.daily_revenue WHERE date BETWEEN $1 AND $2 AND {}",
            clauses.join(" AND ")
        );
        let rows = store
            .rows_unrevised(sql, &binds)
            .await
            .map_err(|e| Failure::Unavailable(DASHBOARD, e))?;
        let row = single(&rows)?;
        return Ok((
            reports::int_req(row, 0)?,
            float_req(row, 1)?,
            reports::int_req(row, 2)?,
            float_req(row, 3)?,
        ));
    }
    // `_build_silver_filtered_summary_queries`, after the routed tree.
    let cats = tree(store, a).await?;
    let mut args = Args::default();
    let wh = line_where(a, true, cats.as_deref(), &mut args);
    let lines = read(
        store,
        DASHBOARD,
        format!(
            "SELECT COUNT(DISTINCT l.order_id) AS total_orders, \
             COALESCE(SUM(l.line_amount), 0) AS total_revenue \
             FROM silver.order_lines l WHERE {wh}"
        ),
        &args.0,
    )
    .await?;
    // Returns are narrowed by date, sales type and source alone.
    let mut ret = Args::default();
    let mut ret_where = vec![
        format!(
            "s.order_date BETWEEN {} AND {}",
            ret.next(Bind::Date(a.start)),
            ret.next(Bind::Date(a.end))
        ),
        "s.is_return".to_owned(),
        "s.is_active_source".to_owned(),
    ];
    if a.sales_type != "all" {
        let at = ret.next(Bind::Text(a.sales_type.clone()));
        ret_where.push(format!("s.sales_type = {at}"));
    }
    ret_where.extend(source_clause(a, "s", &mut ret));
    let returns = read(
        store,
        DASHBOARD,
        format!(
            "SELECT COUNT(DISTINCT s.id), COALESCE(SUM(s.grand_total), 0) \
             FROM silver.orders s WHERE {}",
            ret_where.join(" AND ")
        ),
        &ret.0,
    )
    .await?;
    let (line, back) = (single(&lines)?, single(&returns)?);
    Ok((
        reports::int_req(line, 0)?,
        float_req(line, 1)?,
        reports::int_req(back, 0)?,
        float_req(back, 1)?,
    ))
}

async fn summary(store: &PgReads, a: &Asked) -> Result<Vec<u8>, Failure> {
    let (orders, revenue, returns, returns_revenue) = summary_totals(store, a).await?;
    let avg_check = if orders > 0 {
        PyNum::Float(round_float(revenue / orders as f64, 2))
    } else {
        PyNum::Int(0)
    };
    Ok(json(&Summary {
        total_orders: orders,
        total_revenue: round_float(revenue, 2),
        avg_check,
        total_returns: returns,
        returns_revenue: round_float(returns_revenue, 2),
        start_date: a.start.to_string(),
        end_date: a.end.to_string(),
    }))
}

// ─── /api/sales/by-source ───────────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BySource {
    labels: Vec<&'static str>,
    orders: Vec<i64>,
    revenue: Vec<f64>,
    background_color: Vec<&'static str>,
}

/// The service hands the store no `source_id`: the chart is the split by
/// it.
async fn sales_by_source(store: &PgReads, a: &Asked) -> Result<Vec<u8>, Failure> {
    let mut out = BySource {
        labels: Vec::new(),
        orders: Vec::new(),
        revenue: Vec::new(),
        background_color: Vec::new(),
    };
    if a.filtered() {
        let cats = tree(store, a).await?;
        let mut args = Args::default();
        let wh = line_where(a, false, cats.as_deref(), &mut args);
        let sql = format!(
            "SELECT l.source_id, COUNT(DISTINCT l.order_id) as orders, \
             COALESCE(SUM(l.line_amount), 0) as revenue \
             FROM silver.order_lines l WHERE {wh} \
             GROUP BY l.source_id ORDER BY revenue DESC, l.source_id"
        );
        for row in &read(store, DASHBOARD, sql, &args.0).await? {
            let sid = reports::int_col(row, 0)?;
            // A source the chart has no name for (5) is left out.
            if let Some(&(_, name, color)) = SOURCES.iter().find(|s| Some(s.0) == sid) {
                out.labels.push(name);
                out.orders.push(reports::int_req(row, 1)?);
                out.revenue.push(round_float(float_req(row, 2)?, 2));
                out.background_color.push(color);
            }
        }
        return Ok(json(&out));
    }
    let mut args = Args::default();
    let mut clauses = vec![format!(
        "date BETWEEN {} AND {}",
        args.next(Bind::Date(a.start)),
        args.next(Bind::Date(a.end))
    )];
    if a.sales_type != "all" {
        let at = args.next(Bind::Text(a.sales_type.clone()));
        clauses.push(format!("sales_type = {at}"));
    }
    // `channel_measures(POSTGRES)`: a channel is a dimension there.
    let sql = format!(
        "SELECT \
         COALESCE(SUM(orders_count) FILTER (WHERE source_id = 1), 0), \
         COALESCE(SUM(revenue) FILTER (WHERE source_id = 1), 0), \
         COALESCE(SUM(orders_count) FILTER (WHERE source_id = 2), 0), \
         COALESCE(SUM(revenue) FILTER (WHERE source_id = 2), 0), \
         COALESCE(SUM(orders_count) FILTER (WHERE source_id = 4), 0), \
         COALESCE(SUM(revenue) FILTER (WHERE source_id = 4), 0) \
         FROM gold.daily_revenue WHERE {}",
        clauses.join(" AND ")
    );
    let rows = read(store, DASHBOARD, sql, &args.0).await?;
    let mut data = Vec::with_capacity(SOURCES.len());
    for (k, &(_, name, color)) in SOURCES.iter().enumerate() {
        let (orders, revenue) = match rows.first() {
            Some(row) => (
                reports::int_or_zero(row, 2 * k)?,
                reports::float_or_zero(row, 2 * k + 1)?,
            ),
            None => (0, 0.0),
        };
        data.push((name, color, orders, revenue));
    }
    // `sort(key=revenue, reverse=True)` is stable: a tie keeps 1, 2, 4.
    data.sort_by(|x, y| y.3.partial_cmp(&x.3).unwrap_or(Ordering::Equal));
    for (name, color, orders, revenue) in data {
        if revenue > 0.0 || orders > 0 {
            out.labels.push(name);
            out.orders.push(orders);
            out.revenue.push(round_float(revenue, 2));
            out.background_color.push(color);
        }
    }
    Ok(json(&out))
}

// ─── /api/products/top ──────────────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TopProducts {
    labels: Vec<String>,
    wrapped_labels: Vec<Vec<String>>,
    data: Vec<i64>,
    percentages: Vec<f64>,
    background_color: &'static str,
}

/// `RevenueMixin._wrap_label(text, max_chars=25)`: lengths in code points,
/// words as `str.split()` finds them.
fn wrap_label(text: Option<&str>) -> Vec<String> {
    const MAX: usize = 25;
    let text = match text {
        Some(t) if !t.is_empty() => t,
        _ => return vec!["Unknown".to_owned()],
    };
    if text.chars().count() <= MAX {
        return vec![text.to_owned()];
    }
    let mut lines: Vec<String> = Vec::new();
    let mut current = String::new();
    for word in text.split(py_isspace).filter(|w| !w.is_empty()) {
        // `len(current_line) + len(word) + 1 <= max_chars`.
        if current.chars().count() + word.chars().count() < MAX {
            current = py_strip(&format!("{current} {word}")).to_owned();
        } else {
            if !current.is_empty() {
                lines.push(current);
            }
            current = word.to_owned();
        }
    }
    if !current.is_empty() {
        lines.push(current);
    }
    if lines.len() > 2 {
        let second: String = lines[1].chars().take(MAX - 3).collect();
        lines = vec![lines[0].clone(), format!("{second}...")];
    }
    lines
}

async fn top_products(store: &PgReads, a: &Asked) -> Result<Vec<u8>, Failure> {
    let cats = tree(store, a).await?;
    let mut args = Args::default();
    let wh = line_where(a, true, cats.as_deref(), &mut args);
    let limit_at = args.next(Bind::Int(a.top_limit));
    let sql = format!(
        "SELECT COALESCE(MIN(l.catalog_product_name), MIN(l.product_name)) AS product_name, \
         SUM(l.quantity) AS total_qty FROM silver.order_lines l WHERE {wh} \
         GROUP BY COALESCE(CAST(l.product_id AS VARCHAR), l.product_name) \
         ORDER BY total_qty DESC, product_name LIMIT {limit_at}"
    );
    let rows = read(store, DASHBOARD, sql, &args.0).await?;
    let mut out = TopProducts {
        labels: Vec::with_capacity(rows.len()),
        wrapped_labels: Vec::with_capacity(rows.len()),
        data: Vec::with_capacity(rows.len()),
        percentages: Vec::with_capacity(rows.len()),
        background_color: "#2563EB",
    };
    for row in &rows {
        let name = reports::text(row, 0)?;
        out.labels.push(
            name.clone()
                .filter(|n| !n.is_empty())
                .unwrap_or_else(|| "Unknown".to_owned()),
        );
        out.wrapped_labels.push(wrap_label(name.as_deref()));
        out.data.push(reports::int_req(row, 1)?);
    }
    // `total = sum(data) if data else 1`, then a division by it: a window
    // whose quantities add up to nothing is Python's ZeroDivisionError.
    let total: i64 = out.data.iter().sum();
    if !out.data.is_empty() && total == 0 {
        return Err(Failure::Raised(
            "ZeroDivisionError: the quantities sum to 0",
        ));
    }
    for d in &out.data {
        out.percentages
            .push(round_float(*d as f64 / total as f64 * 100.0, 1));
    }
    Ok(json(&out))
}

// ─── /api/products/performance ──────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TopByRevenue {
    labels: Vec<Option<String>>,
    data: Vec<f64>,
    quantities: Vec<i64>,
    background_color: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CategorySplit {
    labels: Vec<Option<String>>,
    revenue: Vec<f64>,
    quantity: Vec<i64>,
    background_color: Vec<&'static str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PerformanceMetrics {
    total_products: usize,
    total_revenue: PyNum,
    total_quantity: i64,
    avg_product_revenue: PyNum,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Performance {
    top_by_revenue: TopByRevenue,
    category_breakdown: CategorySplit,
    metrics: PerformanceMetrics,
}

/// `category_colors[:n]`.
fn colors<const N: usize>(palette: &[&'static str; N], n: usize) -> Vec<&'static str> {
    palette[..n.min(N)].to_vec()
}

async fn product_performance(store: &PgReads, a: &Asked) -> Result<Vec<u8>, Failure> {
    let mut args = Args::default();
    let wh = line_where(a, true, None, &mut args);
    let top = read(
        store,
        DASHBOARD,
        format!(
            "SELECT COALESCE(MIN(l.catalog_product_name), MIN(l.product_name)) AS product_name, \
             SUM(l.line_amount) AS revenue, SUM(l.quantity) AS quantity \
             FROM silver.order_lines l WHERE {wh} \
             GROUP BY COALESCE(CAST(l.product_id AS VARCHAR), l.product_name) \
             ORDER BY revenue DESC, product_name LIMIT 10"
        ),
        &args.0,
    )
    .await?;
    let split = read(
        store,
        DASHBOARD,
        format!(
            "SELECT COALESCE(l.parent_category_name, l.category_name, 'Other') AS category_name, \
             SUM(l.line_amount) AS revenue, SUM(l.quantity) AS quantity \
             FROM silver.order_lines l WHERE {wh} \
             GROUP BY COALESCE(l.parent_category_name, l.category_name, 'Other') \
             ORDER BY revenue DESC, category_name"
        ),
        &args.0,
    )
    .await?;
    let mut category_breakdown = CategorySplit {
        labels: Vec::with_capacity(split.len()),
        revenue: Vec::with_capacity(split.len()),
        quantity: Vec::with_capacity(split.len()),
        background_color: colors(&CATEGORY_COLORS, split.len()),
    };
    for row in &split {
        category_breakdown.labels.push(reports::text(row, 0)?);
        category_breakdown
            .revenue
            .push(round_float(float_req(row, 1)?, 2));
        category_breakdown.quantity.push(reports::int_req(row, 2)?);
    }
    let mut top_by_revenue = TopByRevenue {
        labels: Vec::with_capacity(top.len()),
        data: Vec::with_capacity(top.len()),
        quantities: Vec::with_capacity(top.len()),
        background_color: "#7C3AED",
    };
    let mut revenues = Vec::with_capacity(top.len());
    for row in &top {
        let revenue = float_req(row, 1)?;
        top_by_revenue.labels.push(reports::text(row, 0)?);
        top_by_revenue.data.push(round_float(revenue, 2));
        top_by_revenue.quantities.push(reports::int_req(row, 2)?);
        revenues.push(PyNum::Float(revenue));
    }
    // `sum(...) if top_results else 0`: the int 0 when nothing sold.
    let total_revenue = py_sum(&revenues);
    let total_quantity: i64 = top_by_revenue.quantities.iter().sum();
    let avg_product_revenue = if top.is_empty() {
        PyNum::Int(0)
    } else {
        PyNum::Float(round_float(total_revenue.as_f64() / top.len() as f64, 2))
    };
    Ok(json(&Performance {
        top_by_revenue,
        category_breakdown,
        metrics: PerformanceMetrics {
            total_products: top.len(),
            total_revenue: round_num(total_revenue, 2),
            total_quantity,
            avg_product_revenue,
        },
    }))
}

// ─── /api/categories/breakdown ──────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Breakdown {
    parent_category: String,
    labels: Vec<Option<String>>,
    revenue: Vec<f64>,
    quantity: Vec<i64>,
    background_color: Vec<&'static str>,
}

/// `get_subcategory_breakdown`: the sources by list rather than by
/// `is_active_source`, the parent's name twice, then brand and promocode.
async fn category_breakdown(store: &PgReads, a: &Asked, parent: &str) -> Result<Vec<u8>, Failure> {
    let mut args = Args::default();
    let mut clauses = vec![
        format!(
            "l.order_date BETWEEN {} AND {}",
            args.next(Bind::Date(a.start)),
            args.next(Bind::Date(a.end))
        ),
        "NOT l.is_return".to_owned(),
        "l.source_id IN (1, 2, 4)".to_owned(),
    ];
    if a.sales_type != "all" {
        let at = args.next(Bind::Text(a.sales_type.clone()));
        clauses.push(format!("l.sales_type = {at}"));
    }
    clauses.extend(source_clause(a, "l", &mut args));
    let parent_at = args.next(Bind::Text(parent.to_owned()));
    let child_at = args.next(Bind::Text(parent.to_owned()));
    let brand = a
        .brand
        .as_ref()
        .map(|brand| format!("AND {}", reports::brand_where(brand, &mut args, "l")))
        .unwrap_or_default();
    let promocode = promocode_clause(a.promocode.as_deref(), &mut args)
        .map(|c| format!("AND {c}"))
        .unwrap_or_default();
    let sql = format!(
        "SELECT l.category_name as subcategory_name, SUM(l.line_amount) as revenue, \
         SUM(l.quantity) as quantity FROM silver.order_lines l WHERE {} \
         AND (l.parent_category_name = {parent_at} \
              OR (l.category_name = {child_at} AND l.parent_category_id IS NULL)) \
         {brand} {promocode} \
         GROUP BY l.category_name ORDER BY revenue DESC, l.category_name",
        clauses.join(" AND ")
    );
    let rows = read(store, DASHBOARD, sql, &args.0).await?;
    let mut out = Breakdown {
        parent_category: parent.to_owned(),
        labels: Vec::with_capacity(rows.len()),
        revenue: Vec::with_capacity(rows.len()),
        quantity: Vec::with_capacity(rows.len()),
        background_color: colors(&CATEGORY_COLORS, rows.len()),
    };
    for row in &rows {
        out.labels.push(reports::text(row, 0)?);
        out.revenue.push(round_float(float_req(row, 1)?, 2));
        out.quantity.push(reports::int_req(row, 2)?);
    }
    Ok(json(&out))
}

// ─── /api/brands/analytics ──────────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BrandsByRevenue {
    labels: Vec<String>,
    data: Vec<f64>,
    quantities: Vec<i64>,
    orders: Vec<i64>,
    background_color: Vec<&'static str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BrandsByQuantity {
    labels: Vec<String>,
    data: Vec<i64>,
    revenue: Vec<f64>,
    background_color: Vec<&'static str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BrandMetrics {
    total_brands: usize,
    top_brand: String,
    top_brand_share: PyNum,
    total_revenue: PyNum,
    total_quantity: i64,
    avg_brand_revenue: PyNum,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BrandAnalytics {
    top_by_revenue: BrandsByRevenue,
    top_by_quantity: BrandsByQuantity,
    metrics: BrandMetrics,
}

struct BrandRow {
    name: String,
    revenue: f64,
    quantity: i64,
    orders: i64,
}

/// `(part / whole * 100) if whole > 0 else 0`, rounded to one place.
fn share(part: f64, whole: PyNum) -> PyNum {
    if whole.as_f64() > 0.0 {
        PyNum::Float(round_float(part / whole.as_f64() * 100.0, 1))
    } else {
        PyNum::Int(0)
    }
}

/// The service hands the store no source: `get_brand_analytics(start, end,
/// sales_type=…)`.
async fn brand_analytics(store: &PgReads, a: &Asked) -> Result<Vec<u8>, Failure> {
    let mut args = Args::default();
    let wh = line_window(a, &mut args).join(" AND ");
    let sql = format!(
        "SELECT COALESCE(l.brand, 'Unknown') AS brand_name, SUM(l.line_amount) AS revenue, \
         SUM(l.quantity) AS quantity, COUNT(DISTINCT l.order_id) AS orders \
         FROM silver.order_lines l WHERE {wh} \
         GROUP BY COALESCE(l.brand, 'Unknown') ORDER BY revenue DESC, brand_name"
    );
    let mut brands = Vec::new();
    for row in &read(store, DASHBOARD, sql, &args.0).await? {
        brands.push(BrandRow {
            name: text_req(row, 0)?,
            revenue: float_req(row, 1)?,
            quantity: reports::int_req(row, 2)?,
            orders: reports::int_req(row, 3)?,
        });
    }
    let top: Vec<&BrandRow> = brands.iter().take(10).collect();
    let top_by_revenue = BrandsByRevenue {
        labels: top.iter().map(|b| b.name.clone()).collect(),
        data: top.iter().map(|b| round_float(b.revenue, 2)).collect(),
        quantities: top.iter().map(|b| b.quantity).collect(),
        orders: top.iter().map(|b| b.orders).collect(),
        background_color: colors(&TEN_COLORS, top.len()),
    };
    // `sorted(key=lambda x: (-int(x[2]), x[0]))`: the name breaks a tie by
    // code point, which is how a Rust `String` compares too.
    let mut by_quantity: Vec<&BrandRow> = brands.iter().collect();
    by_quantity.sort_by(|x, y| {
        y.quantity
            .cmp(&x.quantity)
            .then_with(|| x.name.cmp(&y.name))
    });
    by_quantity.truncate(10);
    let top_by_quantity = BrandsByQuantity {
        labels: by_quantity.iter().map(|b| b.name.clone()).collect(),
        data: by_quantity.iter().map(|b| b.quantity).collect(),
        revenue: by_quantity
            .iter()
            .map(|b| round_float(b.revenue, 2))
            .collect(),
        background_color: colors(&TEN_COLORS, by_quantity.len()),
    };
    let revenues: Vec<PyNum> = brands.iter().map(|b| PyNum::Float(b.revenue)).collect();
    let total_revenue = py_sum(&revenues);
    let total_quantity: i64 = brands.iter().map(|b| b.quantity).sum();
    let unique_brands = brands.iter().filter(|b| b.name != UNKNOWN_BRAND).count();
    let (top_brand, top_brand_revenue) = match brands.first() {
        Some(b) => (b.name.clone(), b.revenue),
        None => ("N/A".to_owned(), 0.0),
    };
    let avg_brand_revenue = if unique_brands > 0 {
        PyNum::Float(round_float(
            total_revenue.as_f64() / unique_brands as f64,
            2,
        ))
    } else {
        PyNum::Int(0)
    };
    Ok(json(&BrandAnalytics {
        top_by_revenue,
        top_by_quantity,
        metrics: BrandMetrics {
            total_brands: unique_brands,
            top_brand,
            top_brand_share: share(top_brand_revenue, total_revenue),
            total_revenue: round_num(total_revenue, 2),
            total_quantity,
            avg_brand_revenue,
        },
    }))
}

// ─── /api/promocodes/analytics ──────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PromosByRevenue {
    labels: Vec<String>,
    data: Vec<f64>,
    orders: Vec<i64>,
    background_color: Vec<&'static str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PromosByOrders {
    labels: Vec<String>,
    data: Vec<i64>,
    revenue: Vec<f64>,
    background_color: Vec<&'static str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PromoRow {
    promocode: String,
    orders: i64,
    revenue: f64,
    unique_customers: i64,
    aov: PyNum,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PromoMetrics {
    total_codes: usize,
    top_code: String,
    top_code_share: PyNum,
    promo_orders: i64,
    promo_revenue: PyNum,
    promo_order_share: PyNum,
    promo_customers: i64,
    promo_aov: PyNum,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PromoAnalytics {
    top_by_revenue: PromosByRevenue,
    top_by_orders: PromosByOrders,
    table: Vec<PromoRow>,
    metrics: PromoMetrics,
}

struct Code {
    code: String,
    orders: i64,
    revenue: f64,
    customers: i64,
}

/// `(a / b * 100) if b > 0 else 0` of two counts, rounded to one place.
fn count_share(part: i64, whole: i64) -> PyNum {
    if whole > 0 {
        PyNum::Float(round_float(part as f64 / whole as f64 * 100.0, 1))
    } else {
        PyNum::Int(0)
    }
}

/// `round(revenue / orders, 2) if orders > 0 else 0`.
fn per_order(revenue: f64, orders: i64) -> PyNum {
    if orders > 0 {
        PyNum::Float(round_float(revenue / orders as f64, 2))
    } else {
        PyNum::Int(0)
    }
}

async fn promocode_analytics(store: &PgReads, a: &Asked) -> Result<Vec<u8>, Failure> {
    // The date window, the two predicates on the code, then the sales type:
    // Python's list order, so the sales type is still the third value.
    let mut args = Args::default();
    let mut clauses = vec![
        format!(
            "s.order_date BETWEEN {} AND {}",
            args.next(Bind::Date(a.start)),
            args.next(Bind::Date(a.end))
        ),
        "NOT s.is_return".to_owned(),
        "s.is_active_source".to_owned(),
        "s.promocode IS NOT NULL".to_owned(),
        "s.promocode != ''".to_owned(),
    ];
    if a.sales_type != "all" {
        let at = args.next(Bind::Text(a.sales_type.clone()));
        clauses.push(format!("s.sales_type = {at}"));
    }
    let sql = format!(
        "SELECT s.promocode, COUNT(DISTINCT s.id) as orders, \
         COALESCE(SUM(s.grand_total), 0) as revenue, \
         COUNT(DISTINCT s.buyer_id) as unique_customers \
         FROM silver.orders s WHERE {} \
         GROUP BY s.promocode ORDER BY revenue DESC, s.promocode",
        clauses.join(" AND ")
    );
    let mut codes = Vec::new();
    for row in &read(store, MARKETING, sql, &args.0).await? {
        codes.push(Code {
            code: text_req(row, 0)?,
            orders: reports::int_req(row, 1)?,
            revenue: float_req(row, 2)?,
            customers: reports::int_req(row, 3)?,
        });
    }
    let mut all = Args::default();
    let mut all_where = vec![
        format!(
            "s.order_date BETWEEN {} AND {}",
            all.next(Bind::Date(a.start)),
            all.next(Bind::Date(a.end))
        ),
        "NOT s.is_return".to_owned(),
        "s.is_active_source".to_owned(),
    ];
    if a.sales_type != "all" {
        let at = all.next(Bind::Text(a.sales_type.clone()));
        all_where.push(format!("s.sales_type = {at}"));
    }
    let totals = read(
        store,
        MARKETING,
        format!(
            "SELECT COUNT(DISTINCT s.id), COALESCE(SUM(s.grand_total), 0) \
             FROM silver.orders s WHERE {}",
            all_where.join(" AND ")
        ),
        &all.0,
    )
    .await?;
    let row = single(&totals)?;
    let all_orders = reports::int_or_zero(row, 0)?;
    let all_revenue = reports::float_or_zero(row, 1)?;

    let promo_orders: i64 = codes.iter().map(|c| c.orders).sum();
    let revenues: Vec<PyNum> = codes.iter().map(|c| PyNum::Float(c.revenue)).collect();
    let promo_revenue = py_sum(&revenues);
    let promo_customers: i64 = codes.iter().map(|c| c.customers).sum();

    let top: Vec<&Code> = codes.iter().take(10).collect();
    let top_by_revenue = PromosByRevenue {
        labels: top.iter().map(|c| c.code.clone()).collect(),
        data: top.iter().map(|c| round_float(c.revenue, 2)).collect(),
        orders: top.iter().map(|c| c.orders).collect(),
        background_color: colors(&TEN_COLORS, top.len()),
    };
    // `sorted(key=orders, reverse=True)` is stable: a tie keeps the revenue
    // order the statement gave.
    let mut by_orders: Vec<&Code> = codes.iter().collect();
    by_orders.sort_by_key(|c| std::cmp::Reverse(c.orders));
    by_orders.truncate(10);
    let top_by_orders = PromosByOrders {
        labels: by_orders.iter().map(|c| c.code.clone()).collect(),
        data: by_orders.iter().map(|c| c.orders).collect(),
        revenue: by_orders
            .iter()
            .map(|c| round_float(c.revenue, 2))
            .collect(),
        background_color: colors(&TEN_COLORS, by_orders.len()),
    };
    let table = codes
        .iter()
        .map(|c| PromoRow {
            promocode: c.code.clone(),
            orders: c.orders,
            revenue: round_float(c.revenue, 2),
            unique_customers: c.customers,
            aov: per_order(c.revenue, c.orders),
        })
        .collect();
    let (top_code, top_code_revenue) = match codes.first() {
        Some(c) => (c.code.clone(), c.revenue),
        None => ("N/A".to_owned(), 0.0),
    };
    Ok(json(&PromoAnalytics {
        top_by_revenue,
        top_by_orders,
        table,
        metrics: PromoMetrics {
            total_codes: codes.len(),
            top_code,
            top_code_share: share(top_code_revenue, PyNum::Float(all_revenue)),
            promo_orders,
            promo_revenue: round_num(promo_revenue, 2),
            promo_order_share: count_share(promo_orders, all_orders),
            promo_customers,
            promo_aov: per_order(promo_revenue.as_f64(), promo_orders),
        },
    }))
}

// ─── /api/returns ───────────────────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReturnRow {
    id: i64,
    date: Option<String>,
    amount: f64,
    status_id: i64,
    status_name: String,
    source: Option<String>,
    buyer_id: Option<i64>,
    buyer_name: Option<String>,
    buyer_phone: Option<String>,
    manager_id: Option<i64>,
    manager_name: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Returns {
    returns: Vec<ReturnRow>,
    count: usize,
    start_date: String,
    end_date: String,
}

/// `_RETURN_ORDERS_SQL` names `{managers}`, so `_dashboard_run` sends it
/// where chain 5 writes, with no fallback and no refusal: a Postgres that
/// cannot answer is the app's 500, not a 503 naming a surface.
async fn returns(store: &PgReads, a: &Asked, limit: i64) -> Result<Vec<u8>, Failure> {
    let mut args = Args::default();
    let mut clauses = vec![
        format!(
            "s.order_date BETWEEN {} AND {}",
            args.next(Bind::Date(a.start)),
            args.next(Bind::Date(a.end))
        ),
        "s.is_return = TRUE".to_owned(),
        "s.is_active_source = TRUE".to_owned(),
    ];
    if a.sales_type != "all" {
        let at = args.next(Bind::Text(a.sales_type.clone()));
        clauses.push(format!("s.sales_type = {at}"));
    }
    let limit_at = args.next(Bind::Int(limit));
    let sql = format!(
        "SELECT s.id, s.order_date, s.grand_total, s.status_id, s.source_name, s.buyer_id, \
         b.full_name AS buyer_name, b.phone AS buyer_phone, s.manager_id, \
         m.name AS manager_name \
         FROM silver.orders s \
         LEFT JOIN bronze.buyers b ON s.buyer_id = b.id \
         LEFT JOIN bronze.managers m ON s.manager_id = m.id \
         WHERE {} ORDER BY s.order_date DESC, s.id DESC LIMIT {limit_at}",
        clauses.join(" AND ")
    );
    let rows = store.rows(sql, &args.0).await.map_err(|e| {
        tracing::error!(error = %e.0, "the returns list raised");
        Failure::Raised("a read of chain 5's tables raises past every handler")
    })?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let status_id = reports::int_req(row, 3)?;
        let date: Option<NaiveDate> = row
            .try_get(1)
            .map_err(|_| Failure::Raised("order_date is not a date"))?;
        out.push(ReturnRow {
            id: reports::int_req(row, 0)?,
            date: date.map(|d| d.to_string()),
            amount: reports::float_or_zero(row, 2)?,
            status_id,
            status_name: STATUS_NAMES
                .iter()
                .find(|(id, _)| *id == status_id)
                .map_or_else(
                    || format!("Status {status_id}"),
                    |(_, name)| (*name).to_owned(),
                ),
            source: reports::text(row, 4)?,
            buyer_id: reports::int_col(row, 5)?,
            buyer_name: reports::text(row, 6)?,
            buyer_phone: reports::text(row, 7)?,
            manager_id: reports::int_col(row, 8)?,
            manager_name: reports::text(row, 9)?,
        });
    }
    Ok(json(&Returns {
        count: out.len(),
        returns: out,
        start_date: a.start.to_string(),
        end_date: a.end.to_string(),
    }))
}

// ─── The entry ──────────────────────────────────────────────────────────────

/// `parent_id: int`, the children's path parameter.
fn parent_id(raw: Option<&str>) -> Result<PyInt, Failure> {
    let mut errors = Vec::new();
    match lax(
        raw.unwrap_or_default().to_owned(),
        "path",
        "parent_id",
        &mut errors,
    ) {
        Some(id) => Ok(id),
        None => Err(Failure::Unprocessable(errors)),
    }
}

pub async fn answer(
    route: Route,
    store: &PgReads,
    raw_query: &str,
    parent: Option<&str>,
    now: DateTime<Utc>,
    admit: impl FnOnce() -> Result<(), Failure>,
) -> Result<Vec<u8>, Failure> {
    match route {
        Route::Categories => {
            admit()?;
            return categories(store, None).await;
        }
        Route::ChildCategories => {
            let id = parent_id(parent)?;
            admit()?;
            // `validate_category_id(parent_id, allow_none=False)`, whose
            // field is still `category_id`.
            validate::category_id(Some(&id)).map_err(Failure::BadRequest)?;
            let PyInt::Small(id) = id else {
                return Err(Failure::Raised("a parent past 64 bits passed its check"));
            };
            return categories(store, Some(id)).await;
        }
        Route::Brands => {
            admit()?;
            return brands(store).await;
        }
        Route::Promocodes => {
            admit()?;
            return promocodes(store).await;
        }
        _ => {}
    }
    let q = parse(route, raw_query)?;
    admit()?;
    let a = asked(route, &q, period::kyiv_today(now))?;
    match route {
        Route::Summary => summary(store, &a).await,
        Route::SalesBySource => sales_by_source(store, &a).await,
        Route::TopProducts => top_products(store, &a).await,
        Route::ProductPerformance => product_performance(store, &a).await,
        Route::CategoryBreakdown => {
            let parent = q.parent_category.as_deref().unwrap_or_default();
            category_breakdown(store, &a, parent).await
        }
        Route::BrandAnalytics => brand_analytics(store, &a).await,
        Route::PromocodeAnalytics => promocode_analytics(store, &a).await,
        Route::Returns => returns(store, &a, q.returns_limit).await,
        Route::Categories | Route::ChildCategories | Route::Brands | Route::Promocodes => Err(
            Failure::Raised("a lookup is answered before its query is read"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_endpoint_has_its_route() {
        for name in [
            "get_categories",
            "get_child_categories",
            "get_brands",
            "get_promocodes",
            "get_sales_by_source",
            "get_top_products",
            "get_summary",
            "get_returns",
            "get_product_performance",
            "get_brand_analytics",
            "get_promocode_analytics",
            "get_subcategory_breakdown",
        ] {
            assert!(
                Route::from_endpoint(&format!("web.routes.api.analytics.{name}")).is_some(),
                "{name}"
            );
        }
        // The trend and the forecast stay where they are.
        assert!(Route::from_endpoint("web.routes.api.analytics.get_revenue_trend").is_none());
        assert!(Route::from_endpoint("web.routes.api.analytics.get_revenue_forecast").is_none());
    }

    #[test]
    fn a_label_wraps_as_cpython_wraps_it() {
        // Each expectation printed by CPython running `_wrap_label`.
        assert_eq!(wrap_label(None), ["Unknown"]);
        assert_eq!(wrap_label(Some("")), ["Unknown"]);
        assert_eq!(wrap_label(Some("Short name")), ["Short name"]);
        assert_eq!(
            wrap_label(Some("Крем для обличчя з гіалуроновою кислотою і SPF 50")),
            ["Крем для обличчя з", "гіалуроновою кислотою ..."]
        );
        // Twenty-five code points fit; the 26th starts a line.
        assert_eq!(
            wrap_label(Some("aaaaaaaaaaaa bbbbbbbbbbbb c")),
            ["aaaaaaaaaaaa bbbbbbbbbbbb", "c"]
        );
        // A no-break space is a separator to `str.split()`.
        assert_eq!(
            wrap_label(Some("Toner\u{a0}with\u{a0}a\u{a0}no-break\u{a0}space")),
            ["Toner with a no-break", "space"]
        );
        // Whitespace alone past the length is no line at all.
        assert!(wrap_label(Some(&" ".repeat(30))).is_empty());
        assert_eq!(wrap_label(Some(&"x".repeat(30))), ["x".repeat(30)]);
    }

    #[test]
    fn the_top_limit_is_validate_limits() {
        let message = |v: PyInt| match validate_top_limit(&v) {
            Err(Failure::BadRequest(m)) => m,
            _ => panic!("a 400"),
        };
        assert_eq!(
            message(PyInt::Small(0)),
            "limit: Must be at least 1 (got: 0)"
        );
        assert_eq!(
            message(PyInt::Small(51)),
            "limit: Cannot exceed 50 (got: 51)"
        );
        assert_eq!(
            message(PyInt::Big("-99999999999999999999".to_owned())),
            "limit: Must be at least 1 (got: -99999999999999999999)"
        );
        assert!(matches!(validate_top_limit(&PyInt::Small(50)), Ok(50)));
    }

    #[test]
    fn the_breakdown_names_its_missing_parent_first() {
        let Err(Failure::Unprocessable(errors)) =
            parse(Route::CategoryBreakdown, "source_id=x&period=week")
        else {
            panic!("a 422")
        };
        let names: Vec<_> = errors.iter().map(|e| e.loc).collect();
        assert_eq!(
            names,
            [("query", "parent_category"), ("query", "source_id")]
        );
        assert_eq!(
            serde_json::to_string(&errors[0]).unwrap(),
            r#"{"type":"missing","loc":["query","parent_category"],"msg":"Field required","input":null}"#
        );
    }

    #[test]
    fn a_route_reads_only_what_its_signature_names() {
        // `category_id=x` is no 422 where the route declares no category.
        assert!(parse(Route::ProductPerformance, "category_id=x").is_ok());
        assert!(parse(Route::BrandAnalytics, "source_id=x&limit=x").is_ok());
        assert!(parse(Route::Returns, "limit=0").is_err());
        assert!(parse(Route::TopProducts, "limit=0").is_ok());
    }
}
