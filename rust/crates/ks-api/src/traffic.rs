//! The traffic routes' seven reads — `/api/traffic/analytics`, `/organic`,
//! `/pixel-only`, `/trend`, `/transactions`, `/utm-campaigns` and `/roas` —
//! answered as `web/routes/api/traffic.py` answers them under
//! `KS_READ_TRAFFIC=postgres`: the statements `core/repositories/traffic.py`
//! renders for Postgres over `silver.orders` and `silver.order_utm` (the
//! revenue Gold and the manual expenses for the ROAS), then the Python over
//! the rows — dicts in insertion order, buyers counted as sets, `sum()`
//! compensated, an `int` 0 where Python never made a float — and the rules
//! of `core.utm_classify` that are read at query time: the tab's platform,
//! an organic order's channel and its GA4-shaped row, a buyer's first
//! channel.
//!
//! `GET /api/traffic/backfill-utm/status` is not here: it reports a job in
//! the Python process's memory, which no other process can see.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use serde::ser::{SerializeMap, Serializer};
use sqlx::Row;
use sqlx::postgres::PgRow;

use crate::params::{FieldError, PyInt, int_message, lax_int};
use crate::period;
use crate::pgread::{Bind, PgReads};
use crate::pyrt::{PyNum, py_isspace, py_lower_strip, py_strip, py_sum, round_float, round_num};
use crate::trend::Failure;
use crate::validate;

/// The surface `read_fallback` names in a 503.
const SURFACE: &str = "traffic";

/// `_PLATFORM_EXPR`: an order the parser never classified falls back on its
/// source.
const PLATFORM_EXPR: &str = "COALESCE(u.platform, \
     CASE s.source_id WHEN 1 THEN 'instagram' WHEN 2 THEN 'telegram' ELSE 'unattributed' END)";
/// `_TRAFFIC_TYPE_EXPR`.
const TRAFFIC_TYPE_EXPR: &str = "COALESCE(u.traffic_type, \
     CASE WHEN s.source_id IN (1, 2) THEN 'organic' ELSE 'unknown' END)";

const TRAFFIC_TYPES: [&str; 6] = [
    "paid_confirmed",
    "paid_likely",
    "manager",
    "organic",
    "pixel_only",
    "unknown",
];
const PLATFORMS: [&str; 10] = [
    "facebook",
    "instagram",
    "google",
    "tiktok",
    "email",
    "telegram",
    "ai",
    "manager",
    "other",
    "unattributed",
];
const SORT_COLUMNS: [&str; 6] = [
    "campaign",
    "utm_source",
    "platform",
    "traffic_type",
    "orders",
    "revenue",
];
/// `ORGANIC_CHANNELS`, the order a revenue tie falls back on.
const ORGANIC_CHANNELS: [&str; 11] = [
    "instagram_direct",
    "instagram_site",
    "instagram_stories",
    "telegram",
    "email_campaigns",
    "email_flows",
    "google_organic",
    "ai",
    "facebook",
    "tiktok",
    "other",
];
/// `ORGANIC_DETAIL_LIMIT`.
const ORGANIC_DETAIL_LIMIT: usize = 20;
/// `BONUS_TIERS`, highest first.
const BONUS_TIERS: [(f64, &str); 5] = [
    (7.0, "plus_30"),
    (6.0, "plus_20"),
    (5.0, "plus_10"),
    (4.0, "base"),
    (0.0, "none"),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    Analytics,
    Organic,
    PixelOnly,
    Trend,
    Transactions,
    UtmCampaigns,
    Roas,
}

impl Route {
    /// The route for a contract endpoint name, if it is one of these.
    pub fn from_endpoint(name: &str) -> Option<Self> {
        Some(match name {
            "web.routes.api.traffic.get_traffic_analytics" => Self::Analytics,
            "web.routes.api.traffic.get_traffic_organic" => Self::Organic,
            "web.routes.api.traffic.get_traffic_pixel_only" => Self::PixelOnly,
            "web.routes.api.traffic.get_traffic_trend" => Self::Trend,
            "web.routes.api.traffic.get_traffic_transactions" => Self::Transactions,
            "web.routes.api.traffic.get_traffic_utm_campaigns" => Self::UtmCampaigns,
            "web.routes.api.traffic.get_traffic_roas" => Self::Roas,
            _ => return None,
        })
    }

    fn paged(self) -> bool {
        matches!(self, Route::Transactions | Route::UtmCampaigns)
    }
}

// ─── A dict in insertion order ──────────────────────────────────────────────

struct OMap<V>(Vec<(String, V)>);

impl<V> OMap<V> {
    fn new() -> Self {
        OMap(Vec::new())
    }

    /// `d.setdefault(key, default())`.
    fn entry(&mut self, key: &str, default: impl FnOnce() -> V) -> &mut V {
        let at = match self.0.iter().position(|(k, _)| k == key) {
            Some(at) => at,
            None => {
                self.0.push((key.to_owned(), default()));
                self.0.len() - 1
            }
        };
        &mut self.0[at].1
    }

    fn get(&self, key: &str) -> Option<&V> {
        self.0.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }
}

impl<V: Serialize> Serialize for OMap<V> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(Some(self.0.len()))?;
        for (k, v) in &self.0 {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

/// A `{'orders': …, 'revenue': …}` accumulator: orders an `int`, revenue a
/// float from `0.0`, added to in row order.
#[derive(Clone, Copy, Default)]
struct Acc {
    orders: i64,
    revenue: f64,
}

/// `{'orders': n, 'revenue': round(r, 2)}` as the routes write a bucket.
#[derive(Clone, Copy, Serialize)]
struct Bucket {
    orders: i64,
    revenue: PyNum,
}

impl Bucket {
    /// `_rounded(bucket)`: revenue to the kopeck, an absent bucket zero.
    fn rounded(acc: Option<&Acc>) -> Self {
        let acc = acc.copied().unwrap_or_default();
        Bucket {
            orders: acc.orders,
            revenue: PyNum::Float(round_float(acc.revenue, 2)),
        }
    }
}

// ─── `core.utm_classify`, the rules read at query time ──────────────────────

/// `tab_platform`: Google split into its ads and its free listings.
fn tab_platform(platform: &str, traffic_type: &str) -> String {
    if platform == "google" {
        if matches!(traffic_type, "paid_confirmed" | "paid_likely") {
            "google_ads".to_owned()
        } else {
            "google_organic".to_owned()
        }
    } else {
        platform.to_owned()
    }
}

/// One letter of `re.compile(r'campaign\s*#', re.IGNORECASE)`: the letter,
/// its capital, and for `i` the two characters Python's `re` takes for it —
/// `İ`, whose simple lower case it is, and `ı`, which sre counts as its
/// equivalent.
fn ignorecase_eq(c: char, letter: char) -> bool {
    c == letter || c == letter.to_ascii_uppercase() || (letter == 'i' && matches!(c, 'İ' | 'ı'))
}

/// `_EMAIL_CAMPAIGN_RE.search(text)`: "campaign", any whitespace as
/// `str.isspace()` has it, then `#`.
fn names_an_email_campaign(text: &str) -> bool {
    let chars: Vec<char> = text.chars().collect();
    let word: Vec<char> = "campaign".chars().collect();
    (0..chars.len()).any(|start| {
        if start + word.len() > chars.len() {
            return false;
        }
        if !word
            .iter()
            .zip(&chars[start..])
            .all(|(l, c)| ignorecase_eq(*c, *l))
        {
            return false;
        }
        let mut at = start + word.len();
        while at < chars.len() && py_isspace(chars[at]) {
            at += 1;
        }
        chars.get(at) == Some(&'#')
    })
}

/// `organic_channel`: which organic channel one order belongs to.
fn organic_channel(
    source_id: Option<i64>,
    tagged: bool,
    platform: &str,
    utm_medium: &str,
    utm_campaign: &str,
) -> &'static str {
    if !tagged {
        return if source_id == Some(2) {
            "telegram"
        } else {
            "instagram_direct"
        };
    }
    let medium = py_lower_strip(utm_medium);
    match platform {
        "instagram" => {
            if medium.starts_with("stories") {
                "instagram_stories"
            } else {
                "instagram_site"
            }
        }
        "email" => {
            if medium == "klaviyo" || medium == "campaign" || names_an_email_campaign(utm_campaign)
            {
                "email_campaigns"
            } else {
                "email_flows"
            }
        }
        "google" => "google_organic",
        "telegram" => "telegram",
        "ai" => "ai",
        "facebook" => "facebook",
        "tiktok" => "tiktok",
        _ => "other",
    }
}

/// `organic_detail`: source and medium lower-cased, campaign and content as
/// written, each stripped.
fn organic_detail(source: &str, medium: &str, campaign: &str, content: &str) -> [String; 4] {
    [
        py_lower_strip(source),
        py_lower_strip(medium),
        py_strip(campaign).to_owned(),
        py_strip(content).to_owned(),
    ]
}

/// `first_order_channel`: `(group, channel)` for a buyer's first order.
fn first_order_channel(
    source_id: Option<i64>,
    tagged: bool,
    platform: &str,
    traffic_type: &str,
    utm_medium: &str,
    utm_campaign: &str,
) -> (String, String) {
    if traffic_type == "organic" {
        let channel = organic_channel(source_id, tagged, platform, utm_medium, utm_campaign);
        return ("organic".to_owned(), channel.to_owned());
    }
    if matches!(traffic_type, "paid_confirmed" | "paid_likely") {
        return ("paid".to_owned(), tab_platform(platform, traffic_type));
    }
    if matches!(traffic_type, "manager" | "pixel_only") {
        return (traffic_type.to_owned(), traffic_type.to_owned());
    }
    let channel = match source_id {
        Some(3) => "opencart",
        Some(5) => "exhibition",
        _ => "unknown",
    };
    ("unknown".to_owned(), channel.to_owned())
}

// ─── The query string ───────────────────────────────────────────────────────

/// The route's parameters after FastAPI, before its own checks.
struct Query {
    period: Option<String>,
    start_date: Option<String>,
    end_date: Option<String>,
    /// Kept past 64 bits: `validate_source_id` refuses it with its value.
    source_id: Option<PyInt>,
    sales_type: String,
    traffic_type: Option<String>,
    platform: Option<String>,
    sort_by: String,
    sort_dir: String,
    limit: i64,
    /// Kept past 64 bits: `validate_db_int` refuses it with its value.
    offset: PyInt,
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

/// pydantic's lax `int`, past 64 bits the digits Python's int would hold.
fn lax(raw: String, name: &'static str, errors: &mut Vec<FieldError>) -> Option<PyInt> {
    match lax_int(&raw) {
        Ok(v) => Some(v),
        Err(kind) => {
            errors.push(field_error(kind, name, int_message(kind), raw));
            None
        }
    }
}

/// `limit: int = Query(50, ge=1, le=200)`; past 64 bits pydantic holds the
/// number to the bound its sign breaks.
fn limit(raw: Option<String>, errors: &mut Vec<FieldError>) -> i64 {
    let Some(raw) = raw else { return 50 };
    let Some(value) = lax(raw.clone(), "limit", errors) else {
        return 50;
    };
    let below = match &value {
        PyInt::Small(v) if (1..=200).contains(v) => return *v,
        PyInt::Small(v) => *v < 1,
        PyInt::Big(digits) => digits.starts_with('-'),
    };
    errors.push(if below {
        field_error(
            "greater_than_equal",
            "limit",
            "Input should be greater than or equal to 1",
            raw,
        )
    } else {
        field_error(
            "less_than_equal",
            "limit",
            "Input should be less than or equal to 200",
            raw,
        )
    });
    50
}

/// `offset: int = Query(0, ge=0)`: no upper bound, so past 64 bits it
/// reaches the route.
fn offset(raw: Option<String>, errors: &mut Vec<FieldError>) -> PyInt {
    let Some(raw) = raw else {
        return PyInt::Small(0);
    };
    let Some(value) = lax(raw.clone(), "offset", errors) else {
        return PyInt::Small(0);
    };
    if !value.is_positive() && value != PyInt::Small(0) {
        errors.push(field_error(
            "greater_than_equal",
            "offset",
            "Input should be greater than or equal to 0",
            raw,
        ));
        return PyInt::Small(0);
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
    // FastAPI reads each signature in its order.
    let period = take("period");
    let start_date = take("start_date");
    let end_date = take("end_date");
    let source_id = match (route, take("source_id")) {
        (Route::Roas, _) | (_, None) => None,
        (_, Some(raw)) => lax(raw, "source_id", &mut errors),
    };
    let sales_type = take("sales_type").unwrap_or_else(|| "retail".to_owned());
    let mut q = Query {
        period,
        start_date,
        end_date,
        source_id,
        sales_type,
        traffic_type: None,
        platform: None,
        sort_by: "revenue".to_owned(),
        sort_dir: "desc".to_owned(),
        limit: 50,
        offset: PyInt::Small(0),
    };
    if route.paged() {
        q.traffic_type = take("traffic_type");
        q.platform = take("platform");
        if route == Route::UtmCampaigns {
            q.sort_by = take("sort_by").unwrap_or_else(|| "revenue".to_owned());
            q.sort_dir = take("sort_dir").unwrap_or_else(|| "desc".to_owned());
        }
        q.limit = limit(take("limit"), &mut errors);
        q.offset = offset(take("offset"), &mut errors);
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

/// `float(x)` of a column Python never finds NULL.
fn f8_req(row: &PgRow, i: usize) -> Result<f64, Failure> {
    f8(row, i)?.ok_or(Failure::Raised("float(None)"))
}

fn text(row: &PgRow, i: usize) -> Result<Option<String>, Failure> {
    row.try_get(i)
        .map_err(|_| Failure::Raised("a column the statement names is not text"))
}

fn flag(row: &PgRow, i: usize) -> Result<Option<bool>, Failure> {
    row.try_get(i)
        .map_err(|_| Failure::Raised("a column the statement reads as a flag is not one"))
}

fn date(row: &PgRow, i: usize) -> Result<NaiveDate, Failure> {
    row.try_get::<NaiveDate, _>(i)
        .map_err(|_| Failure::Raised("a date column is not a date"))
}

async fn read(store: &PgReads, sql: String, binds: &[Bind]) -> Result<Vec<PgRow>, Failure> {
    store
        .rows(sql, binds)
        .await
        .map_err(|e| Failure::Unavailable(SURFACE, e))
}

fn json<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).expect("the body is plain data")
}

/// A WHERE clause with its `?`s numbered as `numbered()` numbers them.
struct Where {
    clauses: Vec<String>,
    binds: Vec<Bind>,
}

impl Where {
    fn new(fixed: &[&str]) -> Self {
        Where {
            clauses: fixed.iter().map(|c| (*c).to_owned()).collect(),
            binds: Vec::new(),
        }
    }

    /// A clause with one `?`, and the value it binds.
    fn with(&mut self, clause: &str, bind: Bind) -> &mut Self {
        let at = self.binds.len() + 1;
        self.clauses
            .push(clause.replacen('?', &format!("${at}"), 1));
        self.binds.push(bind);
        self
    }

    fn sql(&self) -> String {
        self.clauses.join(" AND ")
    }

    /// The next placeholder after the clause's own.
    fn next(&self) -> usize {
        self.binds.len() + 1
    }
}

/// The predicate `refresh_traffic_gold_layer` aggregates over, the window,
/// then the source and the sales type as the routes add them.
fn order_where(
    start: NaiveDate,
    end: NaiveDate,
    dated: bool,
    verdict: Option<&str>,
    source_id: Option<i32>,
    sales_type: &str,
) -> Where {
    let mut w = Where::new(&["NOT s.is_return", "s.is_active_source"]);
    if dated {
        w.clauses.push("s.order_date IS NOT NULL".to_owned());
    }
    w.with("s.order_date >= ?", Bind::Date(start));
    w.with("s.order_date <= ?", Bind::Date(end));
    if let Some(verdict) = verdict {
        w.clauses.push(verdict.to_owned());
    }
    if let Some(source) = source_id {
        w.with("s.source_id = ?", Bind::Int(source.into()));
    }
    if sales_type != "all" {
        w.with("s.sales_type = ?", Bind::Text(sales_type.to_owned()));
    }
    w
}

#[derive(Serialize)]
struct Period {
    start: String,
    end: String,
}

impl Period {
    fn of(start: NaiveDate, end: NaiveDate) -> Self {
        Period {
            start: start.to_string(),
            end: end.to_string(),
        }
    }
}

/// `len(set().union(*sets))`.
fn union_len<'a>(sets: impl IntoIterator<Item = &'a HashSet<Option<i64>>>) -> usize {
    let mut all: HashSet<Option<i64>> = HashSet::new();
    for s in sets {
        all.extend(s.iter().copied());
    }
    all.len()
}

// ─── /traffic/analytics ─────────────────────────────────────────────────────

#[derive(Serialize)]
struct Totals {
    orders: i64,
    revenue: PyNum,
}

#[derive(Serialize)]
struct Summary {
    paid: Bucket,
    paid_confirmed: Bucket,
    paid_likely: Bucket,
    organic: Bucket,
    manager: Bucket,
    pixel_only: Bucket,
    unknown: Bucket,
}

#[derive(Serialize)]
struct NewSummary {
    paid: usize,
    paid_confirmed: usize,
    paid_likely: usize,
    organic: usize,
    manager: usize,
    pixel_only: usize,
    unknown: usize,
}

#[derive(Serialize)]
struct NewCustomers {
    total: usize,
    summary: NewSummary,
    by_platform: OMap<usize>,
    by_traffic_type: OMap<usize>,
}

#[derive(Serialize)]
struct Analytics {
    period: Period,
    totals: Totals,
    summary: Summary,
    by_platform: OMap<Bucket>,
    by_traffic_type: OMap<Bucket>,
    new_customers: NewCustomers,
}

fn rounded_map(accs: &OMap<Acc>) -> OMap<Bucket> {
    OMap(
        accs.0
            .iter()
            .map(|(k, a)| {
                (
                    k.clone(),
                    Bucket {
                        orders: a.orders,
                        revenue: PyNum::Float(round_float(a.revenue, 2)),
                    },
                )
            })
            .collect(),
    )
}

async fn analytics(
    store: &PgReads,
    start: NaiveDate,
    end: NaiveDate,
    source_id: Option<i32>,
    sales_type: &str,
) -> Result<Vec<u8>, Failure> {
    let w = order_where(start, end, true, None, source_id, sales_type);
    let wh = w.sql();
    let rows = read(
        store,
        format!(
            "SELECT {PLATFORM_EXPR} AS platform, {TRAFFIC_TYPE_EXPR} AS traffic_type, \
             COUNT(DISTINCT s.id) AS orders, COALESCE(SUM(s.grand_total), 0) AS revenue, \
             (COALESCE(SUM(s.grand_total), 0))::float8 AS revenue_f \
             FROM silver.orders s LEFT JOIN silver.order_utm u ON s.id = u.order_id \
             WHERE {wh} GROUP BY {PLATFORM_EXPR}, {TRAFFIC_TYPE_EXPR} \
             ORDER BY revenue DESC, platform, traffic_type"
        ),
        &w.binds,
    )
    .await?;
    let new_rows = read(
        store,
        format!(
            "SELECT DISTINCT {PLATFORM_EXPR} AS platform, {TRAFFIC_TYPE_EXPR} AS traffic_type, \
             s.buyer_id FROM silver.orders s LEFT JOIN silver.order_utm u ON s.id = u.order_id \
             WHERE {wh} AND s.is_new_customer"
        ),
        &w.binds,
    )
    .await?;

    let mut new_by_platform: OMap<HashSet<Option<i64>>> = OMap::new();
    let mut new_by_type: OMap<HashSet<Option<i64>>> = OMap::new();
    for r in &new_rows {
        let platform = text(r, 0)?.unwrap_or_default();
        let traffic_type = text(r, 1)?.unwrap_or_default();
        let buyer = int(r, 2)?;
        new_by_platform
            .entry(&tab_platform(&platform, &traffic_type), HashSet::new)
            .insert(buyer);
        new_by_type.entry(&traffic_type, HashSet::new).insert(buyer);
    }
    let new_of = |types: &[&str]| union_len(types.iter().filter_map(|t| new_by_type.get(t)));

    let mut platforms: OMap<Acc> = OMap::new();
    let mut types: OMap<Acc> = OMap::new();
    for r in &rows {
        let platform = text(r, 0)?.unwrap_or_default();
        let traffic_type = text(r, 1)?.unwrap_or_default();
        let orders = int(r, 2)?.unwrap_or(0);
        let revenue = f8_req(r, 4)?;
        let p = platforms.entry(&tab_platform(&platform, &traffic_type), Acc::default);
        p.orders += orders;
        p.revenue += revenue;
        let t = types.entry(&traffic_type, Acc::default);
        t.orders += orders;
        t.revenue += revenue;
    }
    let total_orders: i64 = platforms.0.iter().map(|(_, p)| p.orders).sum();
    let total_revenue = py_sum(
        &platforms
            .0
            .iter()
            .map(|(_, p)| PyNum::Float(p.revenue))
            .collect::<Vec<_>>(),
    );
    let zero = Acc::default();
    let confirmed = types.get("paid_confirmed").copied().unwrap_or(zero);
    let likely = types.get("paid_likely").copied().unwrap_or(zero);
    let manager = types.get("manager").copied().unwrap_or(zero);
    // `traffic_types.get('organic', {}).get('revenue', 0)`: absent, the int 0.
    let organic = types.get("organic").copied();
    let flt = |a: Acc| Bucket {
        orders: a.orders,
        revenue: PyNum::Float(round_float(a.revenue, 2)),
    };
    Ok(json(&Analytics {
        period: Period::of(start, end),
        totals: Totals {
            orders: total_orders,
            revenue: round_num(total_revenue, 2),
        },
        summary: Summary {
            paid: Bucket {
                orders: confirmed.orders + likely.orders,
                revenue: PyNum::Float(round_float(confirmed.revenue + likely.revenue, 2)),
            },
            paid_confirmed: flt(confirmed),
            paid_likely: flt(likely),
            organic: match organic {
                Some(a) => flt(a),
                None => Bucket {
                    orders: 0,
                    revenue: PyNum::Int(0),
                },
            },
            manager: flt(manager),
            pixel_only: Bucket::rounded(types.get("pixel_only")),
            unknown: Bucket::rounded(types.get("unknown")),
        },
        by_platform: rounded_map(&platforms),
        by_traffic_type: rounded_map(&types),
        new_customers: NewCustomers {
            total: union_len(new_by_type.0.iter().map(|(_, s)| s)),
            summary: NewSummary {
                paid: new_of(&["paid_confirmed", "paid_likely"]),
                paid_confirmed: new_of(&["paid_confirmed"]),
                paid_likely: new_of(&["paid_likely"]),
                organic: new_of(&["organic"]),
                manager: new_of(&["manager"]),
                pixel_only: new_of(&["pixel_only"]),
                unknown: new_of(&["unknown"]),
            },
            by_platform: OMap(
                new_by_platform
                    .0
                    .iter()
                    .map(|(k, s)| (k.clone(), s.len()))
                    .collect(),
            ),
            by_traffic_type: OMap(
                new_by_type
                    .0
                    .iter()
                    .map(|(k, s)| (k.clone(), s.len()))
                    .collect(),
            ),
        },
    }))
}

// ─── /traffic/organic ───────────────────────────────────────────────────────

/// `{tags}`: the seven columns an organic order is placed by.
const TAGS: &str = "s.source_id, u.traffic_type IS NOT NULL AS tagged, \
     COALESCE(u.platform, CASE s.source_id WHEN 1 THEN 'instagram' WHEN 2 THEN 'telegram' \
     ELSE 'unattributed' END) AS platform, \
     COALESCE(u.utm_source, '') AS utm_source, COALESCE(u.utm_medium, '') AS utm_medium, \
     COALESCE(u.utm_campaign, '') AS utm_campaign, COALESCE(u.utm_content, '') AS utm_content";

/// `_place(row)`: the channel and the detail row of one organic grouping.
fn place(r: &PgRow) -> Result<(&'static str, [String; 4]), Failure> {
    let source = int(r, 0)?;
    let tagged = flag(r, 1)?.unwrap_or(false);
    let platform = text(r, 2)?.unwrap_or_default();
    let (source_tag, medium, campaign, content) = (
        text(r, 3)?.unwrap_or_default(),
        text(r, 4)?.unwrap_or_default(),
        text(r, 5)?.unwrap_or_default(),
        text(r, 6)?.unwrap_or_default(),
    );
    let channel = organic_channel(source, tagged, &platform, &medium, &campaign);
    Ok((
        channel,
        organic_detail(&source_tag, &medium, &campaign, &content),
    ))
}

type Details = OMap<Vec<([String; 4], Acc)>>;

async fn by_channel(
    store: &PgReads,
    start: NaiveDate,
    end: NaiveDate,
    source_id: Option<i32>,
    sales_type: &str,
) -> Result<(OMap<Acc>, Details), Failure> {
    let organic = format!("{TRAFFIC_TYPE_EXPR} = 'organic'");
    let w = order_where(start, end, true, Some(&organic), source_id, sales_type);
    let rows = read(
        store,
        format!(
            "SELECT {TAGS}, COUNT(DISTINCT s.id) AS orders, \
             (COALESCE(SUM(s.grand_total), 0))::float8 AS revenue_f \
             FROM silver.orders s LEFT JOIN silver.order_utm u ON s.id = u.order_id \
             WHERE {} GROUP BY 1, 2, 3, 4, 5, 6, 7",
            w.sql()
        ),
        &w.binds,
    )
    .await?;
    let mut channels: OMap<Acc> = OMap::new();
    let mut details: Details = OMap::new();
    for r in &rows {
        let (channel, key) = place(r)?;
        let orders = int(r, 7)?.ok_or(Failure::Raised("int(None)"))?;
        let revenue = f8_req(r, 8)?;
        let c = channels.entry(channel, Acc::default);
        c.orders += orders;
        c.revenue += revenue;
        let rows = details.entry(channel, Vec::new);
        let at = match rows.iter().position(|(k, _)| *k == key) {
            Some(at) => at,
            None => {
                rows.push((key, Acc::default()));
                rows.len() - 1
            }
        };
        rows[at].1.orders += orders;
        rows[at].1.revenue += revenue;
    }
    Ok((channels, details))
}

#[derive(Serialize)]
struct OrganicTotals {
    orders: i64,
    revenue: PyNum,
    new_customers: usize,
}

#[derive(Serialize)]
struct DetailRow {
    source: String,
    medium: String,
    campaign: String,
    content: String,
    orders: i64,
    revenue: PyNum,
}

#[derive(Serialize)]
struct Channel {
    channel: String,
    orders: i64,
    revenue: PyNum,
    new_customers: usize,
    previous: Bucket,
    details: Vec<DetailRow>,
    details_total: usize,
}

#[derive(Serialize)]
struct Organic {
    period: Period,
    previous_period: Period,
    totals: OrganicTotals,
    previous_totals: Bucket,
    channels: Vec<Channel>,
}

/// `_total(buckets)`: the orders and a compensated sum of the revenue,
/// rounded.
fn total_of(buckets: &OMap<Acc>) -> Bucket {
    let revenue = py_sum(
        &buckets
            .0
            .iter()
            .map(|(_, b)| PyNum::Float(b.revenue))
            .collect::<Vec<_>>(),
    );
    Bucket {
        orders: buckets.0.iter().map(|(_, b)| b.orders).sum(),
        revenue: PyNum::Float(round_float(revenue.as_f64(), 2)),
    }
}

/// Python's `sorted` on a key of floats and strings: ascending, stable.
fn by_key<T>(items: &mut [T], key: impl Fn(&T) -> (f64, f64, usize, String)) {
    items.sort_by(|a, b| {
        let (ka, kb) = (key(a), key(b));
        ka.0.partial_cmp(&kb.0)
            .unwrap_or(Ordering::Equal)
            .then(ka.1.partial_cmp(&kb.1).unwrap_or(Ordering::Equal))
            .then(ka.2.cmp(&kb.2))
            .then(ka.3.cmp(&kb.3))
    });
}

async fn organic(
    store: &PgReads,
    start: NaiveDate,
    end: NaiveDate,
    source_id: Option<i32>,
    sales_type: &str,
) -> Result<Vec<u8>, Failure> {
    let (prev_start, prev_end) = period::comparison_window(start, end, "previous_period")?;
    let (current, details) = by_channel(store, start, end, source_id, sales_type).await?;
    let (previous, _) = by_channel(store, prev_start, prev_end, source_id, sales_type).await?;

    let organic = format!("{TRAFFIC_TYPE_EXPR} = 'organic'");
    let w = order_where(start, end, true, Some(&organic), source_id, sales_type);
    let new_rows = read(
        store,
        format!(
            "SELECT DISTINCT {TAGS}, s.buyer_id \
             FROM silver.orders s LEFT JOIN silver.order_utm u ON s.id = u.order_id \
             WHERE {} AND s.is_new_customer",
            w.sql()
        ),
        &w.binds,
    )
    .await?;
    let mut new_buyers: OMap<HashSet<Option<i64>>> = OMap::new();
    for r in &new_rows {
        let (channel, _) = place(r)?;
        new_buyers.entry(channel, HashSet::new).insert(int(r, 7)?);
    }

    let rank = |c: &str| {
        ORGANIC_CHANNELS
            .iter()
            .position(|x| *x == c)
            .unwrap_or(ORGANIC_CHANNELS.len())
    };
    let mut names: Vec<String> = current.0.iter().map(|(k, _)| k.clone()).collect();
    for (k, _) in &previous.0 {
        if !names.contains(k) {
            names.push(k.clone());
        }
    }
    let revenue_of = |m: &OMap<Acc>, c: &str| m.get(c).map_or(0.0, |a| a.revenue);
    by_key(&mut names, |c| {
        (
            -revenue_of(&current, c),
            -revenue_of(&previous, c),
            rank(c),
            c.clone(),
        )
    });

    let current_total = total_of(&current);
    let channels = names
        .iter()
        .map(|c| {
            let mut rows: Vec<&([String; 4], Acc)> =
                details.get(c).map_or_else(Vec::new, |v| v.iter().collect());
            rows.sort_by(|(ka, a), (kb, b)| {
                (-a.revenue)
                    .partial_cmp(&-b.revenue)
                    .unwrap_or(Ordering::Equal)
                    .then((-a.orders).cmp(&-b.orders))
                    .then(ka.cmp(kb))
            });
            let here = Bucket::rounded(current.get(c));
            Channel {
                channel: c.clone(),
                orders: here.orders,
                revenue: here.revenue,
                new_customers: new_buyers.get(c).map_or(0, HashSet::len),
                previous: Bucket::rounded(previous.get(c)),
                details: rows
                    .iter()
                    .take(ORGANIC_DETAIL_LIMIT)
                    .map(|([source, medium, campaign, content], v)| DetailRow {
                        source: source.clone(),
                        medium: medium.clone(),
                        campaign: campaign.clone(),
                        content: content.clone(),
                        orders: v.orders,
                        revenue: PyNum::Float(round_float(v.revenue, 2)),
                    })
                    .collect(),
                details_total: details.get(c).map_or(0, Vec::len),
            }
        })
        .collect();
    Ok(json(&Organic {
        period: Period::of(start, end),
        previous_period: Period::of(prev_start, prev_end),
        totals: OrganicTotals {
            orders: current_total.orders,
            revenue: current_total.revenue,
            new_customers: union_len(new_buyers.0.iter().map(|(_, s)| s)),
        },
        previous_totals: total_of(&previous),
        channels,
    }))
}

// ─── /traffic/pixel-only ────────────────────────────────────────────────────

#[derive(Default)]
struct People {
    orders: i64,
    revenue: f64,
    buyers: HashSet<i64>,
    new_buyers: HashSet<i64>,
}

#[derive(Serialize)]
struct PeopleOut {
    orders: i64,
    revenue: PyNum,
    buyers: usize,
}

impl PeopleOut {
    fn of(p: &People) -> Self {
        PeopleOut {
            orders: p.orders,
            revenue: PyNum::Float(round_float(p.revenue, 2)),
            buyers: p.buyers.len(),
        }
    }
}

#[derive(Serialize)]
struct FirstChannel {
    group: String,
    channel: String,
    orders: i64,
    revenue: PyNum,
    buyers: usize,
    new_buyers: usize,
}

#[derive(Serialize)]
struct PixelOnly {
    period: Period,
    totals: PeopleOut,
    new: PeopleOut,
    returning: PeopleOut,
    first_channels: Vec<FirstChannel>,
}

async fn pixel_only(
    store: &PgReads,
    start: NaiveDate,
    end: NaiveDate,
    source_id: Option<i32>,
    sales_type: &str,
) -> Result<Vec<u8>, Failure> {
    let pixel = format!("{TRAFFIC_TYPE_EXPR} = 'pixel_only'");
    let w = order_where(start, end, true, Some(&pixel), source_id, sales_type);
    let rows = read(
        store,
        format!(
            "SELECT p.id, p.buyer_id, p.is_new_customer, p.grand_total::float8 AS grand_total_f, \
             f.source_id, f.tagged, f.platform, f.traffic_type, f.utm_medium, f.utm_campaign \
             FROM (SELECT s.id, s.buyer_id, s.is_new_customer, s.grand_total \
             FROM silver.orders s LEFT JOIN silver.order_utm u ON s.id = u.order_id \
             WHERE {}) AS p \
             LEFT JOIN (SELECT ranked.buyer_id, ranked.source_id, ranked.tagged, \
             ranked.platform, ranked.traffic_type, ranked.utm_medium, ranked.utm_campaign \
             FROM (SELECT s.buyer_id, s.source_id, u.traffic_type IS NOT NULL AS tagged, \
             {PLATFORM_EXPR} AS platform, {TRAFFIC_TYPE_EXPR} AS traffic_type, \
             COALESCE(u.utm_medium, '') AS utm_medium, \
             COALESCE(u.utm_campaign, '') AS utm_campaign, \
             ROW_NUMBER() OVER (PARTITION BY s.buyer_id ORDER BY s.order_date, s.id) AS rn \
             FROM silver.orders s LEFT JOIN silver.order_utm u ON s.id = u.order_id \
             WHERE NOT s.is_return AND s.buyer_id IS NOT NULL \
             AND s.order_date IS NOT NULL) AS ranked \
             WHERE ranked.rn = 1) AS f ON f.buyer_id = p.buyer_id",
            w.sql()
        ),
        &w.binds,
    )
    .await?;
    let (mut totals, mut new, mut returning) =
        (People::default(), People::default(), People::default());
    let mut firsts: Vec<((String, String), People)> = Vec::new();
    for r in &rows {
        let buyer = int(r, 1)?;
        let is_new = flag(r, 2)?.unwrap_or(false);
        let total = f8_req(r, 3)?;
        let key = match text(r, 7)? {
            None => ("unknown".to_owned(), "unknown".to_owned()),
            Some(f_type) => first_order_channel(
                int(r, 4)?,
                flag(r, 5)?.unwrap_or(false),
                &text(r, 6)?.unwrap_or_default(),
                &f_type,
                &text(r, 8)?.unwrap_or_default(),
                &text(r, 9)?.unwrap_or_default(),
            ),
        };
        let at = match firsts.iter().position(|(k, _)| *k == key) {
            Some(at) => at,
            None => {
                firsts.push((key, People::default()));
                firsts.len() - 1
            }
        };
        for bucket in [
            &mut totals,
            if is_new { &mut new } else { &mut returning },
            &mut firsts[at].1,
        ] {
            bucket.orders += 1;
            bucket.revenue += total;
            if let Some(b) = buyer {
                bucket.buyers.insert(b);
            }
        }
        if is_new && let Some(b) = buyer {
            firsts[at].1.new_buyers.insert(b);
        }
    }
    firsts.sort_by(|(ka, a), (kb, b)| {
        (-a.revenue)
            .partial_cmp(&-b.revenue)
            .unwrap_or(Ordering::Equal)
            .then((-(a.buyers.len() as i64)).cmp(&-(b.buyers.len() as i64)))
            .then(ka.cmp(kb))
    });
    Ok(json(&PixelOnly {
        period: Period::of(start, end),
        totals: PeopleOut::of(&totals),
        new: PeopleOut::of(&new),
        returning: PeopleOut::of(&returning),
        first_channels: firsts
            .iter()
            .map(|((group, channel), v)| FirstChannel {
                group: group.clone(),
                channel: channel.clone(),
                orders: v.orders,
                revenue: PyNum::Float(round_float(v.revenue, 2)),
                buyers: v.buyers.len(),
                new_buyers: v.new_buyers.len(),
            })
            .collect(),
    }))
}

// ─── /traffic/trend ─────────────────────────────────────────────────────────

#[derive(Serialize)]
struct Day {
    date: String,
    paid_orders: i64,
    paid_revenue: f64,
    organic_orders: i64,
    organic_revenue: f64,
    other_orders: i64,
    other_revenue: f64,
}

async fn trend(
    store: &PgReads,
    start: NaiveDate,
    end: NaiveDate,
    source_id: Option<i32>,
    sales_type: &str,
) -> Result<Vec<u8>, Failure> {
    let w = order_where(start, end, true, None, source_id, sales_type);
    let rows = read(
        store,
        format!(
            "SELECT s.order_date AS date, {TRAFFIC_TYPE_EXPR} AS traffic_type, \
             COUNT(DISTINCT s.id) AS orders, \
             (COALESCE(SUM(s.grand_total), 0))::float8 AS revenue_f \
             FROM silver.orders s LEFT JOIN silver.order_utm u ON s.id = u.order_id \
             WHERE {} GROUP BY s.order_date, {TRAFFIC_TYPE_EXPR} \
             ORDER BY s.order_date, traffic_type",
            w.sql()
        ),
        &w.binds,
    )
    .await?;
    let mut days: Vec<Day> = Vec::new();
    for r in &rows {
        let d = date(r, 0)?.to_string();
        let traffic_type = text(r, 1)?.unwrap_or_default();
        let orders = int(r, 2)?.unwrap_or(0);
        let revenue = f8_req(r, 3)?;
        let at = match days.iter().position(|x| x.date == d) {
            Some(at) => at,
            None => {
                days.push(Day {
                    date: d,
                    paid_orders: 0,
                    paid_revenue: 0.0,
                    organic_orders: 0,
                    organic_revenue: 0.0,
                    other_orders: 0,
                    other_revenue: 0.0,
                });
                days.len() - 1
            }
        };
        let day = &mut days[at];
        match traffic_type.as_str() {
            "paid_confirmed" | "paid_likely" => {
                day.paid_orders += orders;
                day.paid_revenue += revenue;
            }
            "organic" => {
                day.organic_orders += orders;
                day.organic_revenue += revenue;
            }
            _ => {
                day.other_orders += orders;
                day.other_revenue += revenue;
            }
        }
    }
    // `sorted(daily_data.keys())`: ISO dates sort as text.
    days.sort_by(|a, b| a.date.cmp(&b.date));
    for day in &mut days {
        day.paid_revenue = round_float(day.paid_revenue, 2);
        day.organic_revenue = round_float(day.organic_revenue, 2);
        day.other_revenue = round_float(day.other_revenue, 2);
    }
    // The route wraps the list: `return {"trend": result}`.
    #[derive(Serialize)]
    struct Wrapped {
        trend: Vec<Day>,
    }
    Ok(json(&Wrapped { trend: days }))
}

// ─── /traffic/transactions and /traffic/utm-campaigns ───────────────────────

/// The two tables' WHERE: the window without `order_date IS NOT NULL`,
/// then the verdict and the platform as `?`s.
fn table_where(
    start: NaiveDate,
    end: NaiveDate,
    source_id: Option<i32>,
    sales_type: &str,
    traffic_type: Option<&str>,
    platform: Option<&str>,
) -> Where {
    let mut w = order_where(start, end, false, None, source_id, sales_type);
    if let Some(t) = traffic_type {
        w.with(
            &format!("{TRAFFIC_TYPE_EXPR} = ?"),
            Bind::Text(t.to_owned()),
        );
    }
    if let Some(p) = platform {
        w.with(&format!("{PLATFORM_EXPR} = ?"), Bind::Text(p.to_owned()));
    }
    w
}

#[derive(Serialize)]
struct Evidence {
    field: &'static str,
    value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'static str>,
}

/// `_build_evidence`: why an order was classified, strongest first.
fn evidence(tags: [Option<String>; 8]) -> Vec<Evidence> {
    let [
        utm_source,
        utm_medium,
        utm_campaign,
        utm_content,
        fbp,
        fbc,
        ttp,
        fbclid,
    ] = tags.map(|t| t.filter(|s| !s.is_empty()));
    let mut out = Vec::new();
    let mut add = |field, value: &Option<String>, reason| {
        if let Some(v) = value {
            out.push(Evidence {
                field,
                value: v.clone(),
                reason,
            });
        }
    };
    add("_fbc", &fbc, Some("Ad click tracked"));
    add("fbclid", &fbclid, Some("Facebook click ID"));
    add("utm_source", &utm_source, None);
    add("utm_medium", &utm_medium, None);
    add("utm_campaign", &utm_campaign, None);
    add("utm_content", &utm_content, None);
    if fbp.is_some() && fbc.is_none() && utm_source.is_none() {
        add("_fbp", &fbp, Some("Browser pixel only"));
    } else if fbp.is_some() && fbc.is_some() {
        add("_fbp", &fbp, None);
    }
    add("ttp", &ttp, Some("TikTok pixel"));
    out
}

#[derive(Serialize)]
struct Transaction {
    id: Option<i64>,
    date: String,
    amount: f64,
    source: Option<String>,
    traffic_type: Option<String>,
    platform: Option<String>,
    evidence: Vec<Evidence>,
}

#[derive(Serialize)]
struct Transactions {
    transactions: Vec<Transaction>,
    total: i64,
    limit: i64,
    offset: i64,
}

async fn transactions(
    store: &PgReads,
    w: Where,
    limit: i64,
    offset: i64,
) -> Result<Vec<u8>, Failure> {
    let wh = w.sql();
    let count = read(
        store,
        format!(
            "SELECT COUNT(*) FROM silver.orders s \
             LEFT JOIN silver.order_utm u ON s.id = u.order_id WHERE {wh}"
        ),
        &w.binds,
    )
    .await?;
    let total = match count.first() {
        Some(r) => int(r, 0)?.unwrap_or(0),
        None => 0,
    };
    let at = w.next();
    let mut binds = w.binds.clone();
    binds.extend([Bind::Int(limit), Bind::Int(offset)]);
    let rows = read(
        store,
        format!(
            "SELECT s.id, s.order_date, s.grand_total::float8 AS grand_total_f, s.source_name, \
             {TRAFFIC_TYPE_EXPR} AS traffic_type, {PLATFORM_EXPR} AS platform, \
             u.utm_source, u.utm_medium, u.utm_campaign, u.utm_content, \
             u.fbp, u.fbc, u.ttp, u.fbclid \
             FROM silver.orders s LEFT JOIN silver.order_utm u ON s.id = u.order_id \
             WHERE {wh} ORDER BY s.order_date DESC, s.id DESC LIMIT ${at} OFFSET ${}",
            at + 1
        ),
        &binds,
    )
    .await?;
    let transactions = rows
        .iter()
        .map(|r| {
            let order_date: Option<NaiveDate> = r
                .try_get(1)
                .map_err(|_| Failure::Raised("a date column is not a date"))?;
            Ok(Transaction {
                id: int(r, 0)?,
                // `str(None)` for an order with no date, as the route writes it.
                date: order_date.map_or_else(|| "None".to_owned(), |d| d.to_string()),
                amount: f8_req(r, 2)?,
                source: text(r, 3)?,
                traffic_type: text(r, 4)?,
                platform: text(r, 5)?,
                evidence: evidence([
                    text(r, 6)?,
                    text(r, 7)?,
                    text(r, 8)?,
                    text(r, 9)?,
                    text(r, 10)?,
                    text(r, 11)?,
                    text(r, 12)?,
                    text(r, 13)?,
                ]),
            })
        })
        .collect::<Result<Vec<_>, Failure>>()?;
    Ok(json(&Transactions {
        transactions,
        total,
        limit,
        offset,
    }))
}

#[derive(Serialize)]
struct Campaign {
    campaign: Option<String>,
    utm_source: Option<String>,
    platform: Option<String>,
    traffic_type: Option<String>,
    orders: Option<i64>,
    revenue: f64,
}

#[derive(Serialize)]
struct Campaigns {
    campaigns: Vec<Campaign>,
    total: i64,
    limit: i64,
    offset: i64,
}

async fn campaigns(store: &PgReads, w: Where, q: &Query, offset: i64) -> Result<Vec<u8>, Failure> {
    let wh = w.sql();
    let group = format!("COALESCE(u.utm_campaign, ''), {PLATFORM_EXPR}, {TRAFFIC_TYPE_EXPR}");
    let count = read(
        store,
        format!(
            "SELECT COUNT(*) FROM (SELECT 1 FROM silver.orders s \
             LEFT JOIN silver.order_utm u ON s.id = u.order_id \
             WHERE {wh} GROUP BY {group}) AS groups"
        ),
        &w.binds,
    )
    .await?;
    let total = match count.first() {
        Some(r) => int(r, 0)?.unwrap_or(0),
        None => 0,
    };
    let dir = if q.sort_dir.to_lowercase() == "asc" {
        "ASC"
    } else {
        "DESC"
    };
    let at = w.next();
    let mut binds = w.binds.clone();
    binds.extend([Bind::Int(q.limit), Bind::Int(offset)]);
    // `revenue` stays the numeric the ORDER BY sorts by; the float beside it
    // is what `float(revenue)` reads.
    let rows = read(
        store,
        format!(
            "SELECT COALESCE(u.utm_campaign, '') AS campaign, MIN(u.utm_source) AS utm_source, \
             {PLATFORM_EXPR} AS platform, {TRAFFIC_TYPE_EXPR} AS traffic_type, \
             COUNT(DISTINCT s.id) AS orders, COALESCE(SUM(s.grand_total), 0) AS revenue, \
             (COALESCE(SUM(s.grand_total), 0))::float8 AS revenue_f \
             FROM silver.orders s LEFT JOIN silver.order_utm u ON s.id = u.order_id \
             WHERE {wh} GROUP BY {group} \
             ORDER BY {} {dir}, revenue DESC, campaign, platform, traffic_type \
             LIMIT ${at} OFFSET ${}",
            q.sort_by,
            at + 1
        ),
        &binds,
    )
    .await?;
    let campaigns = rows
        .iter()
        .map(|r| {
            Ok(Campaign {
                campaign: text(r, 0)?,
                utm_source: text(r, 1)?,
                platform: text(r, 2)?,
                traffic_type: text(r, 3)?,
                orders: int(r, 4)?,
                revenue: round_float(f8_req(r, 6)?, 2),
            })
        })
        .collect::<Result<Vec<_>, Failure>>()?;
    Ok(json(&Campaigns {
        campaigns,
        total,
        limit: q.limit,
        offset,
    }))
}

// ─── /traffic/roas ──────────────────────────────────────────────────────────

#[derive(Serialize)]
struct Blended {
    revenue: f64,
    spend: PyNum,
    roas: Option<f64>,
}

#[derive(Serialize)]
struct PlatformRoas {
    paid_revenue: PyNum,
    spend: PyNum,
    roas: Option<f64>,
}

#[derive(Serialize)]
struct Roas {
    blended: Blended,
    by_platform: OMap<PlatformRoas>,
    bonus_tier: Option<&'static str>,
    has_spend_data: bool,
}

async fn roas(
    store: &PgReads,
    start: NaiveDate,
    end: NaiveDate,
    sales_type: &str,
) -> Result<Vec<u8>, Failure> {
    let mut rev = Where::new(&[]);
    rev.with("date >= ?", Bind::Date(start));
    rev.with("date <= ?", Bind::Date(end));
    if sales_type != "all" {
        rev.with("sales_type = ?", Bind::Text(sales_type.to_owned()));
    }
    let total_rows = read(
        store,
        format!(
            "SELECT (COALESCE(SUM(revenue), 0))::float8 FROM gold.daily_revenue \
             WHERE {} AND source_id IS NULL",
            rev.sql()
        ),
        &rev.binds,
    )
    .await?;
    let total_revenue = match total_rows.first() {
        Some(r) => f8_req(r, 0)?,
        None => 0.0,
    };
    let paid = format!("{TRAFFIC_TYPE_EXPR} IN ('paid_confirmed', 'paid_likely')");
    let w = order_where(start, end, true, Some(&paid), None, sales_type);
    let paid_rows = read(
        store,
        format!(
            "SELECT {PLATFORM_EXPR} AS platform, \
             (COALESCE(SUM(s.grand_total), 0))::float8 AS paid_revenue_f \
             FROM silver.orders s LEFT JOIN silver.order_utm u ON s.id = u.order_id \
             WHERE {} GROUP BY {PLATFORM_EXPR} ORDER BY platform",
            w.sql()
        ),
        &w.binds,
    )
    .await?;
    let spend_rows = read(
        store,
        "SELECT platform, (SUM(amount))::float8 AS spend_f FROM app.manual_expenses \
         WHERE expense_date BETWEEN $1 AND $2 AND category = 'marketing' \
         AND platform IS NOT NULL GROUP BY platform ORDER BY platform"
            .to_owned(),
        &[Bind::Date(start), Bind::Date(end)],
    )
    .await?;
    // `{row[0]: float(row[1]) for row in rows}`: a repeated key keeps its
    // place and takes the later value.
    let mut paid_by: OMap<f64> = OMap::new();
    for r in &paid_rows {
        *paid_by.entry(&text(r, 0)?.unwrap_or_default(), || 0.0) = f8_req(r, 1)?;
    }
    let mut spend_by: OMap<f64> = OMap::new();
    for r in &spend_rows {
        *spend_by.entry(&text(r, 0)?.unwrap_or_default(), || 0.0) = f8_req(r, 1)?;
    }
    let total_spend = py_sum(
        &spend_by
            .0
            .iter()
            .map(|(_, v)| PyNum::Float(*v))
            .collect::<Vec<_>>(),
    );
    let blended_roas =
        (total_spend.as_f64() > 0.0).then(|| round_float(total_revenue / total_spend.as_f64(), 2));
    let bonus_tier = blended_roas.and_then(|r| {
        BONUS_TIERS
            .iter()
            .find(|(threshold, _)| r >= *threshold)
            .map(|(_, tier)| *tier)
    });
    let mut all: Vec<String> = spend_by
        .0
        .iter()
        .chain(paid_by.0.iter())
        .map(|(k, _)| k.clone())
        .collect();
    all.sort();
    all.dedup();
    let by_platform = OMap(
        all.into_iter()
            .map(|p| {
                // `.get(platform, 0)`: absent, the int 0.
                let spend = spend_by.get(&p).map_or(PyNum::Int(0), |v| PyNum::Float(*v));
                let paid_rev = paid_by.get(&p).map_or(PyNum::Int(0), |v| PyNum::Float(*v));
                let roas = (spend.as_f64() > 0.0)
                    .then(|| round_float(paid_rev.as_f64() / spend.as_f64(), 2));
                (
                    p,
                    PlatformRoas {
                        paid_revenue: round_num(paid_rev, 2),
                        spend: round_num(spend, 2),
                        roas,
                    },
                )
            })
            .collect(),
    );
    Ok(json(&Roas {
        blended: Blended {
            revenue: round_float(total_revenue, 2),
            spend: round_num(total_spend, 2),
            roas: blended_roas,
        },
        by_platform,
        bonus_tier,
        has_spend_data: total_spend.as_f64() > 0.0,
    }))
}

// ─── The entry point ────────────────────────────────────────────────────────

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
    let bad = Failure::BadRequest;
    validate::period(q.period.as_deref()).map_err(bad)?;
    let source_id = if route == Route::Roas {
        None
    } else {
        validate::source_id(q.source_id.as_ref()).map_err(bad)?
    };
    if route.paged()
        && let Some(refused) = validate::db_int(Some(&q.offset), "offset", 64)
    {
        return Err(bad(refused));
    }
    let sales_type = validate::sales_type(&q.sales_type).map_err(bad)?;
    let traffic_type = q.traffic_type.as_deref().filter(|t| !t.is_empty());
    let platform = q.platform.as_deref().filter(|p| !p.is_empty());
    if route.paged() {
        if let Some(t) = traffic_type
            && !TRAFFIC_TYPES.contains(&t)
        {
            return Err(bad(format!("Invalid traffic_type: {t}")));
        }
        if let Some(p) = platform
            && !PLATFORMS.contains(&p)
        {
            return Err(bad(format!("Invalid platform: {p}")));
        }
        if route == Route::UtmCampaigns {
            if !SORT_COLUMNS.contains(&q.sort_by.as_str()) {
                return Err(bad(format!("Invalid sort_by: {}", q.sort_by)));
            }
            if !matches!(q.sort_dir.as_str(), "asc" | "desc") {
                return Err(bad(format!("Invalid sort_dir: {}", q.sort_dir)));
            }
        }
    }
    let (start, end) = period::parse_period(
        q.period.as_deref(),
        q.start_date.as_deref(),
        q.end_date.as_deref(),
        period::kyiv_today(now),
    )?;
    match route {
        Route::Analytics => analytics(store, start, end, source_id, &sales_type).await,
        Route::Organic => organic(store, start, end, source_id, &sales_type).await,
        Route::PixelOnly => pixel_only(store, start, end, source_id, &sales_type).await,
        Route::Trend => trend(store, start, end, source_id, &sales_type).await,
        Route::Roas => roas(store, start, end, &sales_type).await,
        Route::Transactions | Route::UtmCampaigns => {
            let PyInt::Small(offset) = q.offset else {
                return Err(Failure::Raised("an offset past 64 bits passed its check"));
            };
            let w = table_where(start, end, source_id, &sales_type, traffic_type, platform);
            if route == Route::Transactions {
                transactions(store, w, q.limit, offset).await
            } else {
                campaigns(store, w, &q, offset).await
            }
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
            kinds(
                Route::UtmCampaigns,
                "offset=-1&limit=0&sort_by=x&source_id=y"
            ),
            vec![
                ("int_parsing", "source_id"),
                ("greater_than_equal", "limit"),
                ("greater_than_equal", "offset"),
            ]
        );
        assert!(kinds(Route::Roas, "source_id=x&limit=y").is_empty());
        assert!(kinds(Route::Analytics, "limit=y&offset=z").is_empty());
        assert_eq!(
            kinds(Route::Transactions, "limit=99999999999999999999"),
            vec![("less_than_equal", "limit")]
        );
        let q = parse(Route::Transactions, "offset=99999999999999999999").unwrap();
        assert_eq!(q.offset, PyInt::Big("99999999999999999999".to_owned()));
    }

    #[test]
    fn the_rules_read_at_query_time() {
        assert_eq!(tab_platform("google", "paid_likely"), "google_ads");
        assert_eq!(tab_platform("google", "organic"), "google_organic");
        assert_eq!(
            organic_channel(Some(2), false, "telegram", "", ""),
            "telegram"
        );
        assert_eq!(
            organic_channel(Some(4), false, "x", "", ""),
            "instagram_direct"
        );
        assert_eq!(
            organic_channel(Some(4), true, "instagram", " Stories_x", ""),
            "instagram_stories"
        );
        assert_eq!(
            organic_channel(Some(4), true, "email", "x", "Campaign #12"),
            "email_campaigns"
        );
        assert_eq!(
            organic_channel(Some(4), true, "email", "x", "CAMPAİGN\u{2003}#1"),
            "email_campaigns"
        );
        assert_eq!(
            organic_channel(Some(4), true, "email", "x", "campaign 1"),
            "email_flows"
        );
        assert_eq!(organic_channel(Some(4), true, "ai", "", ""), "ai");
        assert_eq!(organic_channel(Some(4), true, "manager", "", ""), "other");
        assert_eq!(
            organic_detail(" IG ", "Social", " Spring ", "x"),
            [
                "ig".to_owned(),
                "social".to_owned(),
                "Spring".to_owned(),
                "x".to_owned()
            ]
        );
        assert_eq!(
            first_order_channel(Some(3), true, "other", "unknown", "", ""),
            ("unknown".to_owned(), "opencart".to_owned())
        );
    }

    #[test]
    fn evidence_in_its_order() {
        let s = |v: &str| Some(v.to_owned());
        let ev = evidence([s("ig"), None, None, None, s("fb.1"), None, s("t"), None]);
        let fields: Vec<_> = ev.iter().map(|e| (e.field, e.reason)).collect();
        assert_eq!(
            fields,
            vec![("utm_source", None), ("ttp", Some("TikTok pixel"))]
        );
        let ev = evidence([None, None, None, None, s("fb.1"), None, None, None]);
        assert_eq!(ev[0].reason, Some("Browser pixel only"));
    }
}
