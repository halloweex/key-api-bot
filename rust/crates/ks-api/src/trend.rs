//! `GET /api/revenue/trend`: the route (`web/routes/api/analytics.py`), the
//! service and `RevenueMixin.get_revenue_trend`, folded into one function
//! for the branch the pilot serves.
//!
//! WHAT IT SERVES, AND WHAT IT ANSWERS 501
//!
//! The Gold branch: no category, no brand, no promocode, and a source Gold
//! has a channel for (none, 1, 2 or 4). The line-level branch, the
//! promocode branch, source 3 and the forecast overlay are answered 501 —
//! but only after every check Python makes before reaching them, so a
//! request Python refuses is refused here in the same words.

use std::collections::HashMap;
use std::future::Future;

use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;

use crate::params::{self, FieldError};
use crate::period::{self, Raised};
use crate::pyrt::{PyNum, py_sum, round_float, round_num};
use crate::validate;

/// `_GOLD_SOURCE_COLUMNS`: the sources DuckDB's Gold had a column for, and
/// so the sources the Postgres reader is allowed to answer.
const GOLD_SOURCES: [i32; 3] = [1, 2, 4];

/// One row of `fetch_series`: `(date, float(revenue), int(orders_count))`.
#[derive(Clone, Debug, PartialEq)]
pub struct DayRow {
    pub date: NaiveDate,
    pub revenue: f64,
    pub orders: i64,
}

#[derive(Debug)]
pub struct ReadError(pub String);

/// The one thing the endpoint reads. A trait so the logic is tested without
/// a database; `gold::PgGold` is the implementation that runs.
pub trait Gold: Sync {
    fn series(
        &self,
        start: NaiveDate,
        end: NaiveDate,
        sales_type: &str,
        source_id: Option<i32>,
    ) -> impl Future<Output = Result<Vec<DayRow>, ReadError>> + Send;
}

/// Every way the Python app answers this route other than 200.
#[derive(Debug)]
pub enum Failure {
    /// FastAPI's 422: a typed parameter did not parse.
    Unprocessable(Vec<FieldError>),
    /// The route's `HTTPException(400, str(ValidationError))`.
    BadRequest(String),
    /// Python raised outside its handlers: the app's catch-all 500.
    Raised(&'static str),
    /// The read failed and `KS_READ_FALLBACK=off` refuses DuckDB: 503.
    Unavailable(ReadError),
    /// A branch the pilot leaves to Python.
    NotInPilot(&'static str),
}

impl From<Raised> for Failure {
    fn from(raised: Raised) -> Self {
        Failure::Raised(raised.0)
    }
}

#[derive(Serialize)]
struct Dataset<'a> {
    label: &'static str,
    data: &'a [PyNum],
    #[serde(rename = "borderColor")]
    border_color: &'static str,
    #[serde(rename = "backgroundColor")]
    background_color: &'static str,
    fill: bool,
    tension: f64,
    #[serde(rename = "borderWidth")]
    border_width: u8,
    #[serde(rename = "borderDash", skip_serializing_if = "Option::is_none")]
    border_dash: Option<[u8; 2]>,
}

#[derive(Serialize)]
struct Window<'a> {
    start: String,
    end: String,
    #[serde(rename = "type")]
    kind: &'a str,
}

#[derive(Serialize)]
struct Totals {
    current: PyNum,
    previous: PyNum,
    growth_percent: PyNum,
}

#[derive(Serialize)]
struct Comparison<'a> {
    labels: &'a [String],
    revenue: &'a [PyNum],
    orders: [u8; 0],
    period: Window<'a>,
    totals: Totals,
}

/// The response, in the order Python's dict was built — the bytes match
/// orjson's, not just the parsed value.
#[derive(Serialize)]
struct Body<'a> {
    labels: &'a [String],
    revenue: &'a [PyNum],
    orders: &'a [i64],
    datasets: [Dataset<'a>; 2],
    measure: &'static str,
    comparison: Comparison<'a>,
}

fn iso(date: NaiveDate) -> String {
    date.format("%Y-%m-%d").to_string()
}

/// The JSON body for a 200, or the reason there is none.
pub async fn revenue_trend<G: Gold>(
    gold: &G,
    raw_query: &str,
    now: DateTime<Utc>,
) -> Result<Vec<u8>, Failure> {
    let q = params::parse(raw_query).map_err(Failure::Unprocessable)?;
    let bad = Failure::BadRequest;
    validate::period(q.period.as_deref()).map_err(bad)?;
    let source_id = validate::source_id(q.source_id.as_ref()).map_err(bad)?;
    let category = validate::category_id(q.category_id.as_ref()).map_err(bad)?;
    let brand = validate::brand(q.brand.as_deref()).map_err(bad)?;
    let promocode = validate::promocode(q.promocode.as_deref()).map_err(bad)?;
    let sales_type = validate::sales_type(&q.sales_type).map_err(bad)?;
    let compare_type = validate::compare_type(&q.compare_type);

    let today = period::kyiv_today(now);
    let (start, end) = period::parse_period(
        q.period.as_deref(),
        q.start_date.as_deref(),
        q.end_date.as_deref(),
        today,
    )?;

    if category || brand {
        return Err(Failure::NotInPilot("line_level_filter"));
    }
    if promocode {
        return Err(Failure::NotInPilot("promocode_filter"));
    }
    if source_id.is_some_and(|s| !GOLD_SOURCES.contains(&s)) {
        return Err(Failure::NotInPilot("source_without_gold_channel"));
    }

    // Python's order: the current series, then the window, then the
    // previous series — so a read that fails answers 503 before a window
    // that overflows could answer 500.
    let current = gold
        .series(start, end, &sales_type, source_id)
        .await
        .map_err(Failure::Unavailable)?;
    let (prev_start, prev_end) = period::comparison_window(start, end, compare_type)?;
    let previous = gold
        .series(prev_start, prev_end, &sales_type, source_id)
        .await
        .map_err(Failure::Unavailable)?;

    let by_day: HashMap<NaiveDate, (f64, i64)> = current
        .iter()
        .map(|row| (row.date, (row.revenue, row.orders)))
        .collect();
    let (mut labels, mut revenue, mut orders) = (Vec::new(), Vec::new(), Vec::new());
    let mut day = start;
    while day <= end {
        labels.push(day.format("%d.%m").to_string());
        // A day with no row is `(0, 0)`: two ints, and `round(0, 2)` is `0`.
        let (value, count) = match by_day.get(&day) {
            Some(&(value, count)) => (PyNum::Float(round_float(value, 2)), count),
            None => (PyNum::Int(0), 0),
        };
        revenue.push(value);
        orders.push(count);
        // `current += timedelta(days=1)` raises past 9999-12-31, after the
        // last day was already appended — and so does this.
        day = period::add_days(day, 1)?;
    }

    let prev_by_day: HashMap<NaiveDate, f64> =
        previous.iter().map(|row| (row.date, row.revenue)).collect();
    let mut prev = Vec::new();
    let mut day = prev_start;
    while day <= prev_end {
        prev.push(
            prev_by_day
                .get(&day)
                .map_or(PyNum::Int(0), |&value| PyNum::Float(round_float(value, 2))),
        );
        day = period::add_days(day, 1)?;
    }

    let current_total = py_sum(&revenue);
    let prev_total = py_sum(&prev);
    let growth = if prev_total.as_f64() > 0.0 {
        let (c, p) = (current_total.as_f64(), prev_total.as_f64());
        PyNum::Float((c - p) / p * 100.0)
    } else {
        PyNum::Int(0)
    };

    // The route's forecast overlay, attempted under exactly its condition.
    let current_period = matches!(q.period.as_deref(), Some("month" | "week"));
    if q.include_forecast && source_id.is_none() && (current_period || end > today) {
        return Err(Failure::NotInPilot("forecast_overlay"));
    }

    let body = Body {
        labels: &labels,
        revenue: &revenue,
        orders: &orders,
        datasets: [
            Dataset {
                label: "This Period",
                data: &revenue,
                border_color: "#16A34A",
                background_color: "rgba(22, 163, 74, 0.1)",
                fill: true,
                tension: 0.3,
                border_width: 2,
                border_dash: None,
            },
            Dataset {
                label: "Previous Period",
                data: &prev,
                border_color: "#9CA3AF",
                background_color: "rgba(156, 163, 175, 0.1)",
                fill: false,
                tension: 0.3,
                border_width: 2,
                border_dash: Some([5, 5]),
            },
        ],
        measure: "revenue",
        comparison: Comparison {
            labels: &labels,
            revenue: &prev,
            orders: [],
            period: Window {
                start: iso(prev_start),
                end: iso(prev_end),
                kind: compare_type,
            },
            totals: Totals {
                current: round_num(current_total, 2),
                previous: round_num(prev_total, 2),
                growth_percent: round_num(growth, 1),
            },
        },
    };
    Ok(serde_json::to_vec(&body).expect("the body is plain data"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Gold in memory: `(date, sales_type, source, revenue, orders)` rows.
    struct Memory(Vec<(NaiveDate, &'static str, Option<i32>, f64, i64)>);

    impl Gold for Memory {
        fn series(
            &self,
            start: NaiveDate,
            end: NaiveDate,
            sales_type: &str,
            source_id: Option<i32>,
        ) -> impl Future<Output = Result<Vec<DayRow>, ReadError>> + Send {
            let mut by_day: std::collections::BTreeMap<NaiveDate, (f64, i64)> = Default::default();
            for &(date, kind, source, revenue, orders) in &self.0 {
                if start <= date
                    && date <= end
                    && source == source_id
                    && (sales_type == "all" || kind == sales_type)
                {
                    let cell = by_day.entry(date).or_default();
                    cell.0 += revenue;
                    cell.1 += orders;
                }
            }
            let rows = by_day
                .into_iter()
                .map(|(date, (revenue, orders))| DayRow {
                    date,
                    revenue,
                    orders,
                })
                .collect();
            std::future::ready(Ok(rows))
        }
    }

    fn day(s: &str) -> NaiveDate {
        s.parse().unwrap()
    }

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn gold() -> Memory {
        Memory(vec![
            (day("2026-10-19"), "retail", None, 1234.56, 3),
            (day("2026-10-20"), "retail", None, 0.0, 0),
            (day("2026-10-12"), "retail", None, 100.1, 1),
        ])
    }

    async fn body(query: &str) -> serde_json::Value {
        let bytes = revenue_trend(&gold(), query, at("2026-10-20T12:00:00Z"))
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn a_missing_day_is_int_zero_and_a_zero_row_is_float_zero() {
        let text = String::from_utf8(
            revenue_trend(
                &gold(),
                "start_date=2026-10-19&end_date=2026-10-21",
                at("2026-10-20T12:00:00Z"),
            )
            .await
            .unwrap(),
        )
        .unwrap();
        assert!(
            text.starts_with(
                r#"{"labels":["19.10","20.10","21.10"],"revenue":[1234.56,0.0,0],"orders":[3,0,0],"#
            ),
            "{text}"
        );
    }

    #[tokio::test]
    async fn the_week_is_monday_to_today_in_kyiv() {
        let b = body("period=week").await;
        assert_eq!(b["labels"], serde_json::json!(["19.10", "20.10"]));
        assert_eq!(b["comparison"]["period"]["start"], "2026-10-17");
        assert_eq!(b["comparison"]["period"]["end"], "2026-10-18");
    }

    #[tokio::test]
    async fn the_branches_the_pilot_leaves_to_python() {
        let now = at("2026-10-20T12:00:00Z");
        for (query, branch) in [
            ("brand=BrandA", "line_level_filter"),
            ("category_id=4", "line_level_filter"),
            ("promocode=SALE", "promocode_filter"),
            ("source_id=3", "source_without_gold_channel"),
            ("period=month&include_forecast=true", "forecast_overlay"),
        ] {
            match revenue_trend(&gold(), query, now).await {
                Err(Failure::NotInPilot(b)) => assert_eq!(b, branch, "{query}"),
                other => panic!("{query}: {other:?}"),
            }
        }
        // …and the forecast flag where Python would not attempt one.
        assert!(
            revenue_trend(&gold(), "period=last_month&include_forecast=true", now)
                .await
                .is_ok()
        );
    }
}
