//! `/api/products/intel/*`: the /products tab's six product-intelligence GET
//! routes, answered as `web/routes/api/products_intel.py` answers them under
//! `KS_READ_PRODUCTS_INTEL=postgres` — the statements
//! `core/repositories/products_intel.py` renders for Postgres, the Python
//! arithmetic over their rows, the bytes orjson writes.
//!
//! The parity is by contract: the page sends a period or two dates, the
//! sales type and the header's four filters, which these routes ignore, and
//! the pairs widget adds `product_id` — a product a pair named, as decimal
//! digits. `limit` the page never sends, so it, and any other spelling of
//! `product_id`, is read as Rust reads an integer; where pydantic's lax
//! reading answers otherwise, the harness's list says so.

use chrono::{DateTime, NaiveDate, TimeDelta, Utc};
use rust_decimal::Decimal;
use serde::Serialize;
use sqlx::Row;
use sqlx::postgres::PgRow;

use crate::gold::decimal_to_float;
use crate::params::FieldError;
use crate::period;
use crate::pgread::{Bind, PgReads};
use crate::pyrt::{PyNum, round_float};
use crate::trend::Failure;
use crate::validate;

/// The surface `read_fallback` names in a 503.
const SURFACE: &str = "products_intel";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    Summary,
    Pairs,
    Distribution,
    CategoryCombos,
    BrandAffinity,
    Momentum,
}

impl Route {
    /// The route for a contract endpoint name, if it is one of these.
    pub fn from_endpoint(name: &str) -> Option<Self> {
        Some(match name {
            "web.routes.api.products_intel.get_basket_summary" => Self::Summary,
            "web.routes.api.products_intel.get_product_pairs" => Self::Pairs,
            "web.routes.api.products_intel.get_basket_distribution" => Self::Distribution,
            "web.routes.api.products_intel.get_category_combos" => Self::CategoryCombos,
            "web.routes.api.products_intel.get_brand_affinity" => Self::BrandAffinity,
            "web.routes.api.products_intel.get_product_momentum" => Self::Momentum,
            _ => return None,
        })
    }

    /// `limit`'s default and `validate_limit`'s `max_value`, where it has one.
    fn limit(self) -> Option<(i64, i64)> {
        match self {
            Self::Pairs => Some((20, 50)),
            Self::CategoryCombos | Self::BrandAffinity => Some((10, 30)),
            Self::Momentum => Some((5, 20)),
            Self::Summary | Self::Distribution => None,
        }
    }
}

/// The route's parameters after FastAPI, before its own validators.
struct Query {
    period: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
    sales_type: String,
    limit: i64,
    product_id: Option<i64>,
}

/// An `Optional[int] = Query(…)`, read as Rust reads an integer.
fn int(raw: &str, name: &'static str, errors: &mut Vec<FieldError>) -> Option<i64> {
    let parsed = raw.parse::<i64>().ok();
    if parsed.is_none() {
        errors.push(FieldError {
            kind: "int_parsing",
            loc: ("query", name),
            msg: "Input should be a valid integer, unable to parse string as an integer",
            input: raw.to_owned(),
        });
    }
    parsed
}

fn parse(route: Route, raw: &str) -> Result<Query, Failure> {
    let mut last = std::collections::HashMap::new();
    for (name, value) in form_urlencoded::parse(raw.as_bytes()) {
        last.insert(name.into_owned(), value.into_owned());
    }
    let mut take = |name: &str| last.remove(name);
    let mut errors = Vec::new();
    let period = take("period");
    let start_date = take("start_date");
    let end_date = take("end_date");
    let sales_type = take("sales_type").unwrap_or_else(|| "retail".to_owned());
    // FastAPI reads the parameters in the signature's order: limit, then product_id.
    let limit = match route.limit() {
        Some((default, _)) => match take("limit") {
            Some(raw) => int(&raw, "limit", &mut errors).unwrap_or(default),
            None => default,
        },
        None => 0,
    };
    let product_id = match (route, take("product_id")) {
        (Route::Pairs, Some(raw)) => int(&raw, "product_id", &mut errors),
        _ => None,
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
        product_id,
    })
}

/// `core.validators.validate_limit(value, max_value=…)`.
fn validate_limit(value: i64, max: i64) -> Result<i64, String> {
    if value < 1 {
        return Err(format!("limit: Must be at least 1 (got: {value})"));
    }
    if value > max {
        return Err(format!("limit: Cannot exceed {max} (got: {value})"));
    }
    Ok(value)
}

// ─── The statements ─────────────────────────────────────────────────────────
//
// `core/repositories/products_intel.py`'s texts as `render_tables(sql,
// POSTGRES)` and `numbered()` leave them; the line predicate is
// `core.duckdb_constants.line_window_where`.

/// The WHERE clause, its binds, and the next free placeholder.
fn line_window(start: NaiveDate, end: NaiveDate, sales_type: &str) -> (String, Vec<Bind>, usize) {
    let mut clause =
        String::from("l.order_date BETWEEN $1 AND $2 AND NOT l.is_return AND l.is_active_source");
    let mut binds = vec![Bind::Date(start), Bind::Date(end)];
    if sales_type != "all" {
        clause.push_str(" AND l.sales_type = $3");
        binds.push(Bind::Text(sales_type.to_owned()));
    }
    let next = binds.len() + 1;
    (clause, binds, next)
}

fn summary_sql(wh: &str) -> String {
    format!(
        "WITH order_sizes AS (
                SELECT l.order_id, l.order_grand_total AS grand_total,
                       COUNT(DISTINCT COALESCE(l.product_id, l.line_id)) AS item_count
                FROM silver.order_lines l
                WHERE {wh}
                GROUP BY l.order_id, l.order_grand_total
            )
            SELECT
                COALESCE(AVG(item_count), 0) AS avg_basket_size,
                COUNT(*) AS total_orders,
                COUNT(CASE WHEN item_count >= 2 THEN 1 END) AS multi_item_orders,
                COALESCE(SUM(CASE WHEN item_count >= 2 THEN grand_total END), 0) AS multi_revenue,
                COALESCE(SUM(grand_total), 0) AS total_revenue,
                COALESCE(AVG(CASE WHEN item_count >= 2 THEN grand_total END), 0) AS multi_aov,
                COALESCE(AVG(CASE WHEN item_count = 1 THEN grand_total END), 0) AS single_aov
            FROM order_sizes"
    )
}

fn top_pair_sql(wh: &str) -> String {
    format!(
        "WITH oi AS (
                SELECT l.order_id,
                       COALESCE(l.product_id, l.line_id) AS product_id,
                       MIN(l.product_name) AS product_name
                FROM silver.order_lines l
                WHERE {wh}
                GROUP BY l.order_id, COALESCE(l.product_id, l.line_id)
            ),
            multi AS (
                SELECT order_id, product_id, product_name
                FROM oi WHERE order_id IN (
                    SELECT order_id FROM oi GROUP BY order_id HAVING COUNT(*) >= 2
                )
            )
            SELECT
                COALESCE(p_a.name, a.product_name) AS name_a,
                COALESCE(p_b.name, b.product_name) AS name_b,
                COUNT(DISTINCT a.order_id) AS co_occurrence
            FROM multi a
            JOIN multi b ON a.order_id = b.order_id AND a.product_id < b.product_id
            LEFT JOIN bronze.products p_a ON a.product_id = p_a.id
            LEFT JOIN bronze.products p_b ON b.product_id = p_b.id
            GROUP BY name_a, name_b
            ORDER BY co_occurrence DESC, name_a, name_b
            LIMIT 1"
    )
}

fn pairs_sql(wh: &str, having: i64, product_filter: &str, limit_at: usize) -> String {
    format!(
        "WITH order_items AS (
                SELECT l.order_id,
                       COALESCE(l.product_id, l.line_id) AS product_id,
                       MIN(l.product_name) AS product_name
                FROM silver.order_lines l
                WHERE {wh}
                GROUP BY l.order_id, COALESCE(l.product_id, l.line_id)
            ),
            multi_orders AS (
                SELECT order_id, product_id, product_name
                FROM order_items
                WHERE order_id IN (
                    SELECT order_id FROM order_items
                    GROUP BY order_id HAVING COUNT(*) >= 2
                )
            ),
            pair_counts AS (
                SELECT a.product_id AS a_id, b.product_id AS b_id,
                       COUNT(DISTINCT a.order_id) AS co_occurrence
                FROM multi_orders a
                JOIN multi_orders b ON a.order_id = b.order_id
                    AND a.product_id < b.product_id
                GROUP BY a.product_id, b.product_id
                HAVING COUNT(DISTINCT a.order_id) >= {having}
            ),
            product_orders AS (
                SELECT product_id, COUNT(DISTINCT order_id) AS orders
                FROM order_items
                GROUP BY product_id
            ),
            total AS (
                SELECT COUNT(DISTINCT order_id) AS total_orders FROM order_items
            ),
            with_metrics AS (
                SELECT pc.a_id, pc.b_id, pc.co_occurrence,
                       COALESCE(p_a.name, oi_a.product_name, 'Unknown') AS a_name,
                       COALESCE(p_b.name, oi_b.product_name, 'Unknown') AS b_name,
                       po_a.orders AS a_orders,
                       po_b.orders AS b_orders,
                       t.total_orders,
                       pc.co_occurrence * 1.0 / t.total_orders AS support,
                       pc.co_occurrence * 1.0 / po_a.orders AS conf_a_to_b,
                       pc.co_occurrence * 1.0 / po_b.orders AS conf_b_to_a,
                       CASE WHEN po_a.orders * po_b.orders > 0
                           THEN (pc.co_occurrence * t.total_orders * 1.0) / (po_a.orders * po_b.orders)
                           ELSE 0 END AS lift
                FROM pair_counts pc
                LEFT JOIN bronze.products p_a ON pc.a_id = p_a.id
                LEFT JOIN bronze.products p_b ON pc.b_id = p_b.id
                LEFT JOIN (SELECT product_id, MIN(product_name) AS product_name FROM multi_orders GROUP BY product_id) oi_a ON pc.a_id = oi_a.product_id
                LEFT JOIN (SELECT product_id, MIN(product_name) AS product_name FROM multi_orders GROUP BY product_id) oi_b ON pc.b_id = oi_b.product_id
                LEFT JOIN product_orders po_a ON pc.a_id = po_a.product_id
                LEFT JOIN product_orders po_b ON pc.b_id = po_b.product_id
                CROSS JOIN total t
            )
            SELECT a_id, a_name, b_id, b_name,
                   co_occurrence, support, conf_a_to_b, conf_b_to_a, lift,
                   a_orders, b_orders, total_orders
            FROM with_metrics
            {product_filter}
            ORDER BY co_occurrence DESC, a_id, b_id
            LIMIT ${limit_at}"
    )
}

fn distribution_sql(wh: &str) -> String {
    format!(
        "WITH order_sizes AS (
                SELECT l.order_id, l.order_grand_total AS grand_total,
                       COUNT(DISTINCT COALESCE(l.product_id, l.line_id)) AS item_count
                FROM silver.order_lines l
                WHERE {wh}
                GROUP BY l.order_id, l.order_grand_total
            ),
            bucketed AS (
                SELECT
                    CASE
                        WHEN item_count = 1 THEN '1 item'
                        WHEN item_count = 2 THEN '2 items'
                        WHEN item_count = 3 THEN '3 items'
                        WHEN item_count = 4 THEN '4 items'
                        WHEN item_count BETWEEN 5 AND 7 THEN '5-7 items'
                        ELSE '8+ items'
                    END AS bucket,
                    CASE
                        WHEN item_count = 1 THEN 1
                        WHEN item_count = 2 THEN 2
                        WHEN item_count = 3 THEN 3
                        WHEN item_count = 4 THEN 4
                        WHEN item_count BETWEEN 5 AND 7 THEN 5
                        ELSE 6
                    END AS sort_order,
                    grand_total
                FROM order_sizes
            )
            SELECT bucket, sort_order,
                   COUNT(*) AS orders,
                   COALESCE(SUM(grand_total), 0) AS revenue,
                   COALESCE(AVG(grand_total), 0) AS aov
            FROM bucketed
            GROUP BY bucket, sort_order
            ORDER BY sort_order"
    )
}

fn category_combos_sql(wh: &str, limit_at: usize) -> String {
    format!(
        "WITH order_cats AS (
                SELECT DISTINCT l.order_id,
                       COALESCE(l.parent_category_name, l.category_name, 'Unknown') AS category_name,
                       COALESCE(l.parent_category_id, l.category_id) AS category_id
                FROM silver.order_lines l
                WHERE {wh}
            ),
            cat_pairs AS (
                SELECT a.category_name AS cat_a, b.category_name AS cat_b,
                       COUNT(DISTINCT a.order_id) AS co_occurrence
                FROM order_cats a
                JOIN order_cats b ON a.order_id = b.order_id
                    AND a.category_name < b.category_name
                GROUP BY a.category_name, b.category_name
                HAVING COUNT(DISTINCT a.order_id) >= 2
            )
            SELECT cat_a, cat_b, co_occurrence
            FROM cat_pairs
            ORDER BY co_occurrence DESC, cat_a, cat_b
            LIMIT ${limit_at}"
    )
}

fn brand_affinity_sql(wh: &str, having: i64, limit_at: usize) -> String {
    format!(
        "WITH order_brands AS (
                SELECT DISTINCT l.order_id, l.brand
                FROM silver.order_lines l
                WHERE {wh}
                  AND l.brand IS NOT NULL AND l.brand != ''
            ),
            brand_pairs AS (
                SELECT a.brand AS brand_a, b.brand AS brand_b,
                       COUNT(DISTINCT a.order_id) AS co_occurrence,
                       0 AS product_pairs
                FROM order_brands a
                JOIN order_brands b ON a.order_id = b.order_id
                    AND a.brand < b.brand
                GROUP BY a.brand, b.brand
                HAVING COUNT(DISTINCT a.order_id) >= {having}
            )
            SELECT brand_a, brand_b, co_occurrence, product_pairs
            FROM brand_pairs
            ORDER BY co_occurrence DESC, brand_a, brand_b
            LIMIT ${limit_at}"
    )
}

/// Momentum's two windows carry their own predicate, the sales type in each.
fn momentum_sql(sales_type: &str) -> String {
    let (current, previous) = if sales_type == "all" {
        ("BETWEEN $1 AND $2 ", "BETWEEN $3 AND $4 ")
    } else {
        (
            "BETWEEN $1 AND $2 AND l.sales_type = $3",
            "BETWEEN $4 AND $5 AND l.sales_type = $6",
        )
    };
    format!(
        "WITH current_period AS (
                SELECT l.product_id, l.product_name,
                       SUM(l.line_amount) AS revenue,
                       SUM(l.quantity) AS qty
                FROM silver.order_lines l
                WHERE NOT l.is_return AND l.is_active_source
                  AND l.order_date {current}
                GROUP BY l.product_id, l.product_name
                HAVING SUM(l.line_amount) > 0
            ),
            prev_period AS (
                SELECT l.product_id,
                       SUM(l.line_amount) AS revenue,
                       SUM(l.quantity) AS qty
                FROM silver.order_lines l
                WHERE NOT l.is_return AND l.is_active_source
                  AND l.order_date {previous}
                GROUP BY l.product_id
                HAVING SUM(l.line_amount) > 0
            ),
            momentum AS (
                SELECT c.product_id, c.product_name,
                       c.revenue AS current_revenue,
                       COALESCE(p.revenue, 0) AS prev_revenue,
                       c.qty AS current_qty,
                       COALESCE(p.qty, 0) AS prev_qty,
                       CASE
                           WHEN COALESCE(p.revenue, 0) > 0
                           THEN ((c.revenue - p.revenue) / p.revenue) * 100
                           ELSE NULL
                       END AS growth_pct
                FROM current_period c
                LEFT JOIN prev_period p ON c.product_id = p.product_id
                WHERE c.revenue >= 500
            )
            SELECT * FROM momentum
            WHERE growth_pct IS NOT NULL
            ORDER BY growth_pct DESC, product_id, product_name"
    )
}

// ─── Reading a row ──────────────────────────────────────────────────────────

fn column<'r, T>(row: &'r PgRow, i: usize) -> Result<T, Failure>
where
    T: sqlx::Decode<'r, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get(i)
        .map_err(|_| Failure::Raised("a column is not the type the port reads"))
}

/// `float(r[i])` of a numeric: Python's `float(Decimal)`.
fn num(row: &PgRow, i: usize) -> Result<f64, Failure> {
    let value: Option<Decimal> = column(row, i)?;
    value
        .map(decimal_to_float)
        .ok_or(Failure::Raised("float(None)"))
}

/// `float(r[i] or 0)`: a NULL, or a zero, is the float 0.0 either way.
fn num_or_zero(row: &PgRow, i: usize) -> Result<f64, Failure> {
    let value: Option<Decimal> = column(row, i)?;
    Ok(value.map_or(0.0, decimal_to_float))
}

/// `int(r[i])` of a bigint.
fn count(row: &PgRow, i: usize) -> Result<i64, Failure> {
    let value: Option<i64> = column(row, i)?;
    value.ok_or(Failure::Raised("int(None)"))
}

fn text(row: &PgRow, i: usize) -> Result<Option<String>, Failure> {
    column(row, i)
}

// ─── The answers ────────────────────────────────────────────────────────────

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Summary {
    avg_basket_size: f64,
    multi_item_pct: PyNum,
    multi_item_orders: i64,
    total_orders: i64,
    aov_uplift: PyNum,
    multi_aov: f64,
    single_aov: f64,
    top_pair: String,
    top_pair_count: i64,
}

#[derive(Serialize)]
struct Side {
    id: i64,
    name: Option<String>,
    orders: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Pair {
    product_a: Side,
    product_b: Side,
    co_occurrence: i64,
    support: f64,
    #[serde(rename = "confidenceAtoB")]
    confidence_a_to_b: f64,
    #[serde(rename = "confidenceBtoA")]
    confidence_b_to_a: f64,
    lift: f64,
    total_orders: i64,
}

#[derive(Serialize)]
struct Bucket {
    bucket: Option<String>,
    orders: i64,
    revenue: f64,
    aov: f64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CategoryCombo {
    category_a: Option<String>,
    category_b: Option<String>,
    co_occurrence: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BrandPair {
    brand_a: Option<String>,
    brand_b: Option<String>,
    co_occurrence: i64,
    product_pairs: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Mover {
    product_id: Option<i32>,
    product_name: Option<String>,
    current_revenue: f64,
    prev_revenue: f64,
    current_qty: i64,
    prev_qty: i64,
    growth_pct: f64,
}

#[derive(Serialize)]
struct Momentum {
    gainers: Vec<Mover>,
    losers: Vec<Mover>,
}

fn json<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).expect("the body is plain data")
}

fn summary_body(totals: &[PgRow], top: &[PgRow]) -> Result<Vec<u8>, Failure> {
    let row = totals
        .first()
        .ok_or(Failure::Raised("an aggregate returned no row"))?;
    let total_orders = column::<Option<i64>>(row, 1)?.unwrap_or(0);
    let multi_orders = column::<Option<i64>>(row, 2)?.unwrap_or(0);
    let multi_pct = if total_orders > 0 {
        PyNum::Float(round_float(
            multi_orders as f64 / total_orders as f64 * 100.0,
            1,
        ))
    } else {
        PyNum::Int(0)
    };
    let multi_aov = num_or_zero(row, 5)?;
    let single_aov = num_or_zero(row, 6)?;
    let uplift = if single_aov > 0.0 {
        PyNum::Float(round_float(multi_aov / single_aov, 1))
    } else {
        PyNum::Int(0)
    };
    // `f"{top_pair[0]} + {top_pair[1]}"`: a None would print as "None".
    let (top_pair, top_pair_count) = match top.first() {
        Some(pair) => (
            format!(
                "{} + {}",
                text(pair, 0)?.unwrap_or_else(|| "None".to_owned()),
                text(pair, 1)?.unwrap_or_else(|| "None".to_owned())
            ),
            count(pair, 2)?,
        ),
        None => ("N/A".to_owned(), 0),
    };
    Ok(json(&Summary {
        avg_basket_size: round_float(num_or_zero(row, 0)?, 1),
        multi_item_pct: multi_pct,
        multi_item_orders: multi_orders,
        total_orders,
        aov_uplift: uplift,
        multi_aov: round_float(multi_aov, 0),
        single_aov: round_float(single_aov, 0),
        top_pair,
        top_pair_count,
    }))
}

fn pairs_body(rows: &[PgRow]) -> Result<Vec<u8>, Failure> {
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        out.push(Pair {
            product_a: Side {
                id: count(r, 0)?,
                name: text(r, 1)?,
                orders: count(r, 9)?,
            },
            product_b: Side {
                id: count(r, 2)?,
                name: text(r, 3)?,
                orders: count(r, 10)?,
            },
            co_occurrence: count(r, 4)?,
            support: round_float(num(r, 5)?, 4),
            confidence_a_to_b: round_float(num(r, 6)?, 3),
            confidence_b_to_a: round_float(num(r, 7)?, 3),
            lift: round_float(num(r, 8)?, 2),
            total_orders: count(r, 11)?,
        });
    }
    Ok(json(&out))
}

fn distribution_body(rows: &[PgRow]) -> Result<Vec<u8>, Failure> {
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        out.push(Bucket {
            bucket: text(r, 0)?,
            orders: count(r, 2)?,
            revenue: round_float(num(r, 3)?, 0),
            aov: round_float(num(r, 4)?, 0),
        });
    }
    Ok(json(&out))
}

fn combos_body(rows: &[PgRow]) -> Result<Vec<u8>, Failure> {
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        out.push(CategoryCombo {
            category_a: text(r, 0)?,
            category_b: text(r, 1)?,
            co_occurrence: count(r, 2)?,
        });
    }
    Ok(json(&out))
}

fn brands_body(rows: &[PgRow]) -> Result<Vec<u8>, Failure> {
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let pairs: Option<i32> = column(r, 3)?;
        out.push(BrandPair {
            brand_a: text(r, 0)?,
            brand_b: text(r, 1)?,
            co_occurrence: count(r, 2)?,
            product_pairs: i64::from(pairs.ok_or(Failure::Raised("int(None)"))?),
        });
    }
    Ok(json(&out))
}

fn momentum_body(rows: &[PgRow], limit: i64) -> Result<Vec<u8>, Failure> {
    let (mut gainers, mut losers) = (Vec::new(), Vec::new());
    for r in rows {
        let growth: Option<Decimal> = column(r, 6)?;
        let growth = growth.ok_or(Failure::Raised("float(None)"))?;
        let mover = Mover {
            product_id: column(r, 0)?,
            product_name: text(r, 1)?,
            current_revenue: round_float(num(r, 2)?, 0),
            prev_revenue: round_float(num(r, 3)?, 0),
            current_qty: count(r, 4)?,
            prev_qty: count(r, 5)?,
            growth_pct: round_float(decimal_to_float(growth), 1),
        };
        // `if r[6] > 0 … elif r[6] < 0`: the Decimal, before rounding.
        if growth > Decimal::ZERO {
            gainers.push(mover);
        } else if growth < Decimal::ZERO {
            losers.push(mover);
        }
    }
    // `list.sort(key=growthPct[, reverse=True])`: stable on the rounded value.
    gainers.sort_by(|a, b| b.growth_pct.total_cmp(&a.growth_pct));
    losers.sort_by(|a, b| a.growth_pct.total_cmp(&b.growth_pct));
    let cut = usize::try_from(limit).unwrap_or(0);
    gainers.truncate(cut);
    losers.truncate(cut);
    Ok(json(&Momentum { gainers, losers }))
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
    let bad = Failure::BadRequest;
    validate::period(q.period.as_deref()).map_err(bad)?;
    let sales_type = validate::sales_type(&q.sales_type).map_err(bad)?;
    let limit = match route.limit() {
        Some((_, max)) => validate_limit(q.limit, max).map_err(bad)?,
        None => 0,
    };
    let (start, end) = period::parse_period(
        q.period.as_deref(),
        q.start_date.as_deref(),
        q.end_date.as_deref(),
        period::kyiv_today(now),
    )?;
    let unavailable = |e| Failure::Unavailable(SURFACE, e);
    let (wh, mut binds, next) = line_window(start, end, &sales_type);
    // Dynamic threshold: >= 2 for 14+ day ranges, >= 1 for shorter.
    let having = if (end - start).num_days() + 1 >= 14 {
        2
    } else {
        1
    };
    match route {
        Route::Summary => {
            let totals = store
                .rows(summary_sql(&wh), &binds)
                .await
                .map_err(unavailable)?;
            let top = store
                .rows(top_pair_sql(&wh), &binds)
                .await
                .map_err(unavailable)?;
            summary_body(&totals, &top)
        }
        Route::Pairs => {
            let (filter, limit_at) = match q.product_id {
                Some(id) => {
                    binds.push(Bind::Int(id));
                    binds.push(Bind::Int(id));
                    (
                        format!("WHERE a_id = ${next} OR b_id = ${}", next + 1),
                        next + 2,
                    )
                }
                None => (String::new(), next),
            };
            binds.push(Bind::Int(limit));
            let sql = pairs_sql(&wh, having, &filter, limit_at);
            pairs_body(&store.rows(sql, &binds).await.map_err(unavailable)?)
        }
        Route::Distribution => {
            let rows = store
                .rows(distribution_sql(&wh), &binds)
                .await
                .map_err(unavailable)?;
            distribution_body(&rows)
        }
        Route::CategoryCombos => {
            binds.push(Bind::Int(limit));
            let sql = category_combos_sql(&wh, next);
            combos_body(&store.rows(sql, &binds).await.map_err(unavailable)?)
        }
        Route::BrandAffinity => {
            binds.push(Bind::Int(limit));
            let sql = brand_affinity_sql(&wh, having, next);
            brands_body(&store.rows(sql, &binds).await.map_err(unavailable)?)
        }
        Route::Momentum => {
            let days = (end - start).num_days() + 1;
            let prev_start = start
                .checked_sub_signed(TimeDelta::days(days))
                .ok_or(Failure::Raised("date out of range"))?;
            let prev_end = start
                .checked_sub_signed(TimeDelta::days(1))
                .ok_or(Failure::Raised("date out of range"))?;
            let mut binds = vec![Bind::Date(start), Bind::Date(end)];
            if sales_type != "all" {
                binds.push(Bind::Text(sales_type.clone()));
            }
            binds.push(Bind::Date(prev_start));
            binds.push(Bind::Date(prev_end));
            if sales_type != "all" {
                binds.push(Bind::Text(sales_type.clone()));
            }
            let rows = store
                .rows(momentum_sql(&sales_type), &binds)
                .await
                .map_err(unavailable)?;
            momentum_body(&rows, limit)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kinds(route: Route, query: &str) -> Vec<&'static str> {
        match parse(route, query) {
            Err(Failure::Unprocessable(errors)) => errors.iter().map(|e| e.kind).collect(),
            Err(other) => panic!("{other:?}"),
            Ok(_) => vec![],
        }
    }

    #[test]
    fn limit_and_product_id_are_read_only_where_they_exist() {
        assert_eq!(
            kinds(Route::Pairs, "limit=x&product_id=y"),
            ["int_parsing", "int_parsing"]
        );
        assert!(kinds(Route::Summary, "limit=x&product_id=y").is_empty());
        assert!(kinds(Route::Momentum, "product_id=y").is_empty());
        assert_eq!(parse(Route::Pairs, "").unwrap().limit, 20);
        assert_eq!(parse(Route::CategoryCombos, "").unwrap().limit, 10);
        assert_eq!(parse(Route::Momentum, "").unwrap().limit, 5);
        assert_eq!(
            parse(Route::Pairs, "product_id=7").unwrap().product_id,
            Some(7)
        );
    }

    #[test]
    fn validate_limit_says_what_python_says() {
        assert_eq!(
            validate_limit(0, 50).unwrap_err(),
            "limit: Must be at least 1 (got: 0)"
        );
        assert_eq!(
            validate_limit(51, 50).unwrap_err(),
            "limit: Cannot exceed 50 (got: 51)"
        );
        assert_eq!(validate_limit(50, 50), Ok(50));
    }

    #[test]
    fn the_placeholders_follow_the_sales_type() {
        assert_eq!(line_window(NaiveDate::MIN, NaiveDate::MAX, "all").2, 3);
        assert_eq!(line_window(NaiveDate::MIN, NaiveDate::MAX, "retail").2, 4);
        assert!(momentum_sql("all").contains("BETWEEN $3 AND $4"));
        assert!(momentum_sql("b2b").contains("l.sales_type = $6"));
        assert!(pairs_sql("w", 2, "WHERE a_id = $4 OR b_id = $5", 6).ends_with("LIMIT $6"));
    }
}
