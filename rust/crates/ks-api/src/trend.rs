//! `GET /api/revenue/trend`: the route (`web/routes/api/analytics.py`), the
//! service and `RevenueMixin.get_revenue_trend`, folded into one function
//! for the branch the pilot serves, with the stored forecast laid over it.
//!
//! WHAT IT SERVES, AND WHAT IT ANSWERS 501
//!
//! The Gold branch: no category, no brand, no promocode, and a source Gold
//! has a channel for (none, 1, 2 or 4) — and on it the forecast overlay,
//! read from `app.revenue_predictions` as the route reads it. The
//! line-level branch, the promocode branch and source 3 are answered 501 —
//! but only after every check Python makes before reaching them, so a
//! request Python refuses is refused here in the same words.

use std::collections::HashMap;
use std::future::Future;

use chrono::{DateTime, Datelike, NaiveDate, Utc};
use serde::Serialize;

use crate::handover::Reason;
use crate::params::{self, FieldError};
use crate::period::{self, Raised, Refused};
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

/// `core.prediction_service.FORECAST_HORIZON_DAYS`: how far ahead a stored
/// forecast is shown.
const FORECAST_HORIZON_DAYS: i64 = 60;

/// One row of `get_stored_forecast`'s statement, as Postgres holds it:
/// every column but the date may be NULL.
#[derive(Clone, Debug, PartialEq)]
pub struct PredictionRow {
    pub date: NaiveDate,
    pub revenue: Option<f64>,
    pub mae: Option<f64>,
    pub mape: Option<f64>,
    pub wape: Option<f64>,
    pub created_at: Option<DateTime<Utc>>,
}

/// Why a read returned no rows.
#[derive(Debug)]
pub enum ReadError {
    /// It never reached its data — another revision, a revision check
    /// Postgres did not answer, no connection in time: the request is handed
    /// to the Python app, which may well answer it.
    HandOver(Reason, String),
    /// The statement itself failed: Python's 503 under `KS_READ_FALLBACK=off`.
    Failed(String),
}

impl ReadError {
    /// What a read Python does not guard ends in: the app's 500 — unless
    /// the read never reached its data, which hands the request over like
    /// any other read.
    pub fn raised(self, why: &'static str) -> Failure {
        match self {
            ReadError::HandOver(reason, detail) => Failure::HandOver(reason, detail),
            ReadError::Failed(_) => Failure::Raised(why),
        }
    }
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::HandOver(reason, why) => write!(f, "{}: {why}", reason.as_str()),
            ReadError::Failed(why) => f.write_str(why),
        }
    }
}

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

    /// `GoalsMixin.get_stored_forecast`: the stored predictions for
    /// `sales_type` from `start` to `end`, by date.
    fn stored_forecast(
        &self,
        start: NaiveDate,
        end: NaiveDate,
        sales_type: &str,
    ) -> impl Future<Output = Result<Vec<PredictionRow>, ReadError>> + Send;
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
    Unavailable(&'static str, ReadError),
    /// A branch the pilot leaves to Python.
    NotInPilot(&'static str),
    /// A request the pilot hands to Python (`crate::handover`).
    HandOver(Reason, String),
    /// slowapi's 429, carrying `str(limit)`.
    RateLimited(String),
}

impl From<Raised> for Failure {
    fn from(raised: Raised) -> Self {
        Failure::Raised(raised.0)
    }
}

/// `parse_period` sits inside the route's `try` since #308: its
/// `ValidationError` is the route's 400 like any validator's.
impl From<Refused> for Failure {
    fn from(refused: Refused) -> Self {
        match refused {
            Refused::Invalid(detail) => Failure::BadRequest(detail),
            Refused::Raised(raised) => raised.into(),
        }
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
    datasets: Vec<Dataset<'a>>,
    measure: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    granularity: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    comparison: Option<Comparison<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    forecast: Option<Forecast>,
}

#[derive(Serialize)]
struct Prediction {
    date: String,
    predicted_revenue: PyNum,
    model_mae: PyNum,
    model_mape: PyNum,
    model_wape: PyNum,
}

#[derive(Serialize)]
struct Metrics {
    mae: PyNum,
    mape: PyNum,
    wape: PyNum,
}

/// `get_range_forecast`'s dict once `stack_forecast_on_actuals` updated it:
/// its own keys where they were, `actual_to_date` added last.
#[derive(Serialize)]
struct Forecast {
    predicted_remaining: PyNum,
    predicted_total: PyNum,
    daily_predictions: Vec<Prediction>,
    model_metrics: Metrics,
    last_trained: Option<String>,
    actual_to_date: PyNum,
}

/// `core.filters.by_month`: every calendar month the range touches, oldest
/// first, as `(MM.YYYY, revenue, orders)` summed over its days in range, a
/// day with no row `(0.0, 0)`.
fn by_month(
    start: NaiveDate,
    end: NaiveDate,
    daily: &HashMap<NaiveDate, (f64, i64)>,
) -> Result<Vec<(String, f64, i64)>, Raised> {
    let mut months: Vec<(String, f64, i64)> = Vec::new();
    let mut day = start;
    while day <= end {
        let (value, count) = daily.get(&day).copied().unwrap_or((0.0, 0));
        let label = day.format("%m.%Y").to_string();
        match months.last_mut() {
            Some(month) if month.0 == label => {
                month.1 += value;
                month.2 += count;
            }
            _ => {
                // The bucket starts at `[0.0, 0]`: `0.0 + -0.0` is `0.0`.
                let mut total = 0.0;
                total += value;
                months.push((label, total, count));
            }
        }
        day = period::add_days(day, 1)?;
    }
    Ok(months)
}

fn iso(date: NaiveDate) -> String {
    date.format("%Y-%m-%d").to_string()
}

/// `last_day_of_period`'s month: `(start.replace(day=28) + timedelta(4))
/// .replace(day=1) - timedelta(1)`.
fn last_day_of_month(day: NaiveDate) -> Result<NaiveDate, Raised> {
    let late = period::add_days(day.with_day(28).expect("every month has a 28th"), 4)?;
    period::add_days(late.with_day(1).expect("every month has a first day"), -1)
}

/// `dashboard_service.fit_trend`: lay the trend out over the days the chart
/// draws. The forecast's days past the series come after it, at zero
/// revenue and orders; beside every day the comparison holds the day at the
/// same position of its window — day `d` beside position `d − start` — so
/// the window, which runs over the whole period, is cut back to the days
/// drawn, and a position past it draws 0.
fn fit_trend(
    labels: &mut Vec<String>,
    revenue: &mut Vec<PyNum>,
    orders: &mut Vec<i64>,
    comparison: Option<&mut Vec<PyNum>>,
    start: NaiveDate,
    forecast_days: &[NaiveDate],
) {
    let drawn = i64::try_from(labels.len()).expect("a range is at most 3 660 days");
    let positions: Vec<i64> = (0..drawn)
        .chain(
            forecast_days
                .iter()
                .map(|day| day.signed_duration_since(start).num_days()),
        )
        .collect();
    for day in forecast_days {
        labels.push(day.format("%d.%m").to_string());
        revenue.push(PyNum::Int(0));
        orders.push(0);
    }
    if let Some(window) = comparison {
        // A forecast day is never before `start`: no position is negative.
        let laid = positions
            .iter()
            .map(|&k| {
                usize::try_from(k)
                    .ok()
                    .and_then(|k| window.get(k))
                    .copied()
                    .unwrap_or(PyNum::Int(0))
            })
            .collect();
        *window = laid;
    }
}

/// `get_range_forecast(start, period_end)` with the badge
/// `stack_forecast_on_actuals` lays over the series, and the forecast's
/// days; None where the route answers without an overlay. The route's
/// `except` drops the overlay on any error, so the one read that is not
/// answered here is a read that never reached its data: that hands the
/// request over, like every read the pilot makes.
async fn stored_forecast<G: Gold>(
    gold: &G,
    start: NaiveDate,
    period_end: NaiveDate,
    today: NaiveDate,
    sales_type: &str,
    series: &[PyNum],
) -> Result<Option<(Forecast, Vec<NaiveDate>)>, Failure> {
    // Today included: its full-day forecast stacks on its actual so far.
    let first = start.max(today);
    let Ok(horizon) = period::add_days(today, FORECAST_HORIZON_DAYS) else {
        return Ok(None);
    };
    let last = period_end.min(horizon);
    if first > last {
        return Ok(None);
    }
    let rows = match gold.stored_forecast(first, last, sales_type).await {
        Ok(rows) => rows,
        Err(ReadError::HandOver(reason, detail)) => return Err(Failure::HandOver(reason, detail)),
        Err(ReadError::Failed(why)) => {
            tracing::warn!(why = why.as_str(), "Forecast unavailable");
            return Ok(None);
        }
    };
    // `float(None)` raises on a day stored without a forecast, and the
    // overlay goes with it.
    let Some(values) = rows
        .iter()
        .map(|row| row.revenue)
        .collect::<Option<Vec<f64>>>()
    else {
        tracing::warn!("Forecast unavailable: a stored day has no predicted revenue");
        return Ok(None);
    };
    let Some(head) = rows.first() else {
        return Ok(None);
    };
    // `float(x) if x else 0`: an absent or zero metric is the int 0.
    let metric = |m: Option<f64>| match m {
        Some(x) if x != 0.0 => PyNum::Float(x),
        _ => PyNum::Int(0),
    };

    // `stack_forecast_on_actuals`: a day stands at its actual, a forecast
    // day at the larger of its actual and its forecast.
    let mut actual: HashMap<NaiveDate, f64> = HashMap::new();
    let mut actuals = Vec::with_capacity(series.len());
    for (i, value) in series.iter().enumerate() {
        let i = i64::try_from(i).expect("a range is at most 3 660 days");
        let Ok(day) = period::add_days(start, i) else {
            return Ok(None);
        };
        // `float(v or 0)`: an int 0, a 0.0 and a -0.0 all stand at 0.0.
        let v = value.as_f64();
        let v = if v == 0.0 { 0.0 } else { v };
        actual.insert(day, v);
        actuals.push(PyNum::Float(v));
    }
    let gaps: Vec<PyNum> = rows
        .iter()
        .zip(&values)
        .map(|(row, &value)| {
            let gap = value - actual.get(&row.date).copied().unwrap_or(0.0);
            PyNum::Float(if gap > 0.0 { gap } else { 0.0 })
        })
        .collect();
    let actual_to_date = round_num(py_sum(&actuals), 2);
    let remaining = round_num(py_sum(&gaps), 2);
    let forecast = Forecast {
        predicted_remaining: remaining,
        predicted_total: round_num(
            PyNum::Float(actual_to_date.as_f64() + remaining.as_f64()),
            2,
        ),
        daily_predictions: rows
            .iter()
            .zip(&values)
            .map(|(row, &value)| Prediction {
                date: iso(row.date),
                predicted_revenue: PyNum::Float(value),
                model_mae: metric(row.mae),
                model_mape: metric(row.mape),
                model_wape: metric(row.wape),
            })
            .collect(),
        model_metrics: Metrics {
            mae: metric(head.mae),
            mape: metric(head.mape),
            wape: metric(head.wape),
        },
        // `_trained_on`: the Kyiv day of the latest stamp among the rows.
        last_trained: rows
            .iter()
            .filter_map(|row| row.created_at)
            .max()
            .map(|stamp| iso(period::kyiv_today(stamp))),
        actual_to_date,
    };
    Ok(Some((forecast, rows.iter().map(|row| row.date).collect())))
}

/// The JSON body for a 200, or the reason there is none.
///
/// `admit` runs where slowapi's check does: after FastAPI parsed the query
/// (so a 422 costs nothing) and before the route's own validation.
pub async fn revenue_trend<G: Gold>(
    gold: &G,
    raw_query: &str,
    now: DateTime<Utc>,
    admit: impl FnOnce() -> Result<(), Failure>,
) -> Result<Vec<u8>, Failure> {
    let q = params::parse(raw_query).map_err(Failure::Unprocessable)?;
    admit()?;
    let bad = Failure::BadRequest;
    validate::period(q.period.as_deref()).map_err(bad)?;
    validate::granularity(&q.granularity).map_err(bad)?;
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

    // A monthly answer and the whole history carry no comparison
    // (`has_comparison`, on the period as sent). The window is computed
    // first all the same, before any read, as `get_revenue_trend` does since
    // step 14: a window that overflows answers 500 before a read that fails
    // could answer 503.
    let monthly = q.granularity == "monthly";
    let include_comparison = q.period.as_deref() != Some("all_time") && !monthly;
    // This month and this week end today, and the chart draws them to their
    // last day: the comparison is the whole period's, day by day, and the
    // growth badge counts the days through today on both sides
    // (`last_day_of_period`, the owner's decision of 2026-11-12).
    let period_end = match q.period.as_deref() {
        Some("month") => last_day_of_month(start)?,
        Some("week") => period::add_days(start, 6)?,
        _ => end,
    };
    let through = end.min(today);
    let (prev_start, prev_end) =
        period::comparison_window(start, end.max(period_end), compare_type)?;
    let current = gold
        .series(start, end, &sales_type, source_id)
        .await
        .map_err(|e| Failure::Unavailable("dashboard", e))?;
    let previous = if include_comparison {
        gold.series(prev_start, prev_end, &sales_type, source_id)
            .await
            .map_err(|e| Failure::Unavailable("dashboard", e))?
    } else {
        Vec::new()
    };

    let by_day: HashMap<NaiveDate, (f64, i64)> = current
        .iter()
        .map(|row| (row.date, (row.revenue, row.orders)))
        .collect();
    let (mut labels, mut revenue, mut orders) = (Vec::new(), Vec::new(), Vec::new());
    let mut day = start;
    if monthly {
        for (label, value, count) in by_month(start, end, &by_day)? {
            labels.push(label);
            revenue.push(PyNum::Float(round_float(value, 2)));
            orders.push(count);
        }
        day = period::add_days(end, 1)?;
    }
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
    while include_comparison && day <= prev_end {
        prev.push(
            prev_by_day
                .get(&day)
                .map_or(PyNum::Int(0), |&value| PyNum::Float(round_float(value, 2))),
        );
        day = period::add_days(day, 1)?;
    }

    // The totals add up the positions through today on both sides
    // (`through`, which the route always passes).
    let counted = usize::try_from(through.signed_duration_since(start).num_days() + 1)
        .unwrap_or(0)
        .min(revenue.len());
    let current_total = py_sum(&revenue[..counted]);
    let prev_total = py_sum(&prev[..counted.min(prev.len())]);
    let growth = if prev_total.as_f64() > 0.0 {
        let (c, p) = (current_total.as_f64(), prev_total.as_f64());
        PyNum::Float((c - p) / p * 100.0)
    } else {
        PyNum::Int(0)
    };

    // The route's forecast overlay, attempted under exactly its condition:
    // the stored forecast of the period's days from today on, stacked on the
    // series before the days ahead are appended to it.
    let current_period = matches!(q.period.as_deref(), Some("month" | "week"));
    let mut forecast = None;
    let mut forecast_days = Vec::new();
    if q.include_forecast
        && !monthly
        && source_id.is_none()
        && (current_period || end > today)
        && let Some((found, days)) =
            stored_forecast(gold, start, period_end, today, &sales_type, &revenue).await?
    {
        forecast_days = days.into_iter().filter(|day| *day > end).collect();
        forecast = Some(found);
    }
    fit_trend(
        &mut labels,
        &mut revenue,
        &mut orders,
        include_comparison.then_some(&mut prev),
        start,
        &forecast_days,
    );

    let body = Body {
        labels: &labels,
        revenue: &revenue,
        orders: &orders,
        datasets: [
            Some(Dataset {
                label: "This Period",
                data: &revenue,
                border_color: "#16A34A",
                background_color: "rgba(22, 163, 74, 0.1)",
                fill: true,
                tension: 0.3,
                border_width: 2,
                border_dash: None,
            }),
            include_comparison.then_some(Dataset {
                label: "Previous Period",
                data: &prev,
                border_color: "#9CA3AF",
                background_color: "rgba(156, 163, 175, 0.1)",
                fill: false,
                tension: 0.3,
                border_width: 2,
                border_dash: Some([5, 5]),
            }),
        ]
        .into_iter()
        .flatten()
        .collect(),
        measure: "revenue",
        granularity: monthly.then_some("monthly"),
        comparison: include_comparison.then_some(Comparison {
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
        }),
        forecast,
    };
    Ok(serde_json::to_vec(&body).expect("the body is plain data"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Gold in memory: `(date, sales_type, source, revenue, orders)` rows,
    /// and the stored forecast as `(sales_type, row)`.
    struct Memory(
        Vec<(NaiveDate, &'static str, Option<i32>, f64, i64)>,
        Vec<(&'static str, PredictionRow)>,
    );

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

        fn stored_forecast(
            &self,
            start: NaiveDate,
            end: NaiveDate,
            sales_type: &str,
        ) -> impl Future<Output = Result<Vec<PredictionRow>, ReadError>> + Send {
            let rows = self
                .1
                .iter()
                .filter(|(kind, row)| *kind == sales_type && start <= row.date && row.date <= end)
                .map(|(_, row)| row.clone())
                .collect();
            std::future::ready(Ok(rows))
        }
    }

    /// A store whose forecast read fails: before reaching its data (`true`)
    /// or in its statement.
    struct Unread(bool);

    impl Gold for Unread {
        fn series(
            &self,
            _start: NaiveDate,
            _end: NaiveDate,
            _sales_type: &str,
            _source_id: Option<i32>,
        ) -> impl Future<Output = Result<Vec<DayRow>, ReadError>> + Send {
            std::future::ready(Ok(Vec::new()))
        }

        fn stored_forecast(
            &self,
            _start: NaiveDate,
            _end: NaiveDate,
            _sales_type: &str,
        ) -> impl Future<Output = Result<Vec<PredictionRow>, ReadError>> + Send {
            std::future::ready(Err(if self.0 {
                ReadError::HandOver(Reason::Pool, "no connection in time".into())
            } else {
                ReadError::Failed("relation does not exist".into())
            }))
        }
    }

    fn day(s: &str) -> NaiveDate {
        s.parse().unwrap()
    }

    fn at(s: &str) -> DateTime<Utc> {
        s.parse().unwrap()
    }

    fn gold() -> Memory {
        Memory(
            vec![
                (day("2026-10-19"), "retail", None, 1234.56, 3),
                (day("2026-10-20"), "retail", None, 0.0, 0),
                (day("2026-10-12"), "retail", None, 100.1, 1),
            ],
            Vec::new(),
        )
    }

    async fn body(query: &str) -> serde_json::Value {
        let bytes = revenue_trend(&gold(), query, at("2026-10-20T12:00:00Z"), || Ok(()))
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
                || Ok(()),
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
    async fn monthly_and_the_whole_history_carry_no_comparison() {
        let b = body("start_date=2026-09-30&end_date=2026-10-20&granularity=monthly").await;
        assert_eq!(b["labels"], serde_json::json!(["09.2026", "10.2026"]));
        assert_eq!(b["granularity"], "monthly");
        assert!(b.get("comparison").is_none());
        assert_eq!(b["datasets"].as_array().unwrap().len(), 1);
        let all = body("period=all_time").await;
        assert_eq!(all["labels"][0], "02.12");
        assert!(all.get("comparison").is_none() && all.get("granularity").is_none());
    }

    #[tokio::test]
    async fn the_week_is_monday_to_today_in_kyiv() {
        let b = body("period=week").await;
        assert_eq!(b["labels"], serde_json::json!(["19.10", "20.10"]));
        // The window is the whole week's, laid by position under the days
        // drawn; the totals count Monday and Tuesday on both sides.
        assert_eq!(b["comparison"]["period"]["start"], "2026-10-12");
        assert_eq!(b["comparison"]["period"]["end"], "2026-10-18");
        assert_eq!(b["comparison"]["revenue"], serde_json::json!([100.1, 0]));
        assert_eq!(b["comparison"]["totals"]["previous"], 100.1);
    }

    #[tokio::test]
    async fn the_branches_the_pilot_leaves_to_python() {
        let now = at("2026-10-20T12:00:00Z");
        for (query, branch) in [
            ("brand=BrandA", "line_level_filter"),
            ("category_id=4", "line_level_filter"),
            ("promocode=SALE", "promocode_filter"),
            ("source_id=3", "source_without_gold_channel"),
        ] {
            match revenue_trend(&gold(), query, now, || Ok(())).await {
                Err(Failure::NotInPilot(b)) => assert_eq!(b, branch, "{query}"),
                other => panic!("{query}: {other:?}"),
            }
        }
        // …and the forecast flag where Python would not attempt one.
        assert!(
            revenue_trend(
                &gold(),
                "period=last_month&include_forecast=true",
                now,
                || Ok(())
            )
            .await
            .is_ok()
        );
    }

    fn prediction(
        date: &str,
        revenue: Option<f64>,
        mae: Option<f64>,
        stamp: Option<&str>,
    ) -> PredictionRow {
        PredictionRow {
            date: day(date),
            revenue,
            mae,
            mape: mae.map(|_| 12.5),
            wape: Some(0.0),
            created_at: stamp.map(at),
        }
    }

    fn with_forecast(stored: Vec<(&'static str, PredictionRow)>) -> Memory {
        let mut store = gold();
        store.1 = stored;
        store
    }

    #[tokio::test]
    async fn the_month_is_drawn_to_its_end_with_the_forecast_stacked_on_it() {
        // Tuesday 20 October in Kyiv: a forecast for today, and for every
        // day to the 31st but the 25th; and one for b2b, not asked.
        let mut stored = vec![(
            "retail",
            prediction(
                "2026-10-20",
                Some(500.0),
                None,
                Some("2026-10-18T21:30:00Z"),
            ),
        )];
        for d in (21..=31).filter(|d| *d != 25) {
            let date = format!("2026-10-{d}");
            stored.push((
                "retail",
                prediction(&date, Some(100.0), Some(10.0), Some("2026-10-17T09:00:00Z")),
            ));
        }
        stored.push(("b2b", prediction("2026-10-21", Some(7.0), None, None)));
        let bytes = revenue_trend(
            &with_forecast(stored),
            "period=month&include_forecast=true",
            at("2026-10-20T12:00:00Z"),
            || Ok(()),
        )
        .await
        .unwrap();
        let b: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let labels = b["labels"].as_array().unwrap();
        assert_eq!(labels.len(), 30, "1–20 October, then 21–31 but the 25th");
        assert_eq!(
            (labels[20].as_str(), labels[29].as_str()),
            (Some("21.10"), Some("31.10"))
        );
        assert!(!labels.contains(&serde_json::json!("25.10")));
        assert_eq!(b["revenue"][29], 0);
        assert_eq!(b["comparison"]["labels"], b["labels"]);
        assert_eq!(b["datasets"][1]["data"], b["comparison"]["revenue"]);
        assert_eq!(b["comparison"]["revenue"].as_array().unwrap().len(), 30);
        // The window is the thirty-one days before 1–31 October.
        assert_eq!(b["comparison"]["period"]["start"], "2026-08-31");
        assert_eq!(b["comparison"]["period"]["end"], "2026-09-30");
        let text = String::from_utf8(bytes).unwrap();
        // Today's 500 over its actual 0, then ten days of 100 with no actual.
        assert!(
            text.contains(concat!(
                r#""forecast":{"predicted_remaining":1500.0,"predicted_total":2834.66,"#,
                r#""daily_predictions":[{"date":"2026-10-20","predicted_revenue":500.0,"#,
                r#""model_mae":0,"model_mape":0,"model_wape":0}"#
            )),
            "{text}"
        );
        // 21:30 UTC on the 18th is the 19th in Kyiv; the first row's metrics.
        assert!(
            text.ends_with(concat!(
                r#""model_metrics":{"mae":0,"mape":0,"wape":0},"#,
                r#""last_trained":"2026-10-19","actual_to_date":1334.66}}"#
            )),
            "{text}"
        );
    }

    #[tokio::test]
    async fn a_day_stored_without_a_forecast_drops_the_overlay_not_the_chart() {
        let store = with_forecast(vec![
            ("retail", prediction("2026-10-21", Some(100.0), None, None)),
            ("retail", prediction("2026-10-22", None, None, None)),
        ]);
        let b: serde_json::Value = serde_json::from_slice(
            &revenue_trend(
                &store,
                "period=week&include_forecast=true",
                at("2026-10-20T12:00:00Z"),
                || Ok(()),
            )
            .await
            .unwrap(),
        )
        .unwrap();
        assert!(b.get("forecast").is_none());
        assert_eq!(b["labels"], serde_json::json!(["19.10", "20.10"]));
    }

    #[tokio::test]
    async fn a_forecast_read_hands_over_only_when_it_never_reached_its_data() {
        let (now, query) = (
            at("2026-10-20T12:00:00Z"),
            "period=month&include_forecast=true",
        );
        match revenue_trend(&Unread(true), query, now, || Ok(())).await {
            Err(Failure::HandOver(..)) => {}
            other => panic!("{other:?}"),
        }
        let b: serde_json::Value = serde_json::from_slice(
            &revenue_trend(&Unread(false), query, now, || Ok(()))
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(b.get("forecast").is_none());
        assert_eq!(b["labels"].as_array().unwrap().len(), 20);
    }
}
