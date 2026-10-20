//! Dates as `core/filters.parse_period` and the trend method compute them.
//!
//! "Today" is Kyiv's, read off the instant, never the process's zone — the
//! Python container runs `TZ=Europe/Kyiv` and CI runs UTC, and the code is
//! right on both because it names the zone. So does this.
//!
//! Python's `date` spans years 1 to 9999 and raises past either end; chrono's
//! spans far more and would answer. The route turns the Python error into a
//! 500, so every step here is checked against Python's range, not chrono's.

use chrono::{DateTime, Datelike, Months, NaiveDate, TimeDelta, Utc};
use chrono_tz::Europe::Kyiv;

use crate::strptime::strptime_ymd;

/// Where the Python code would have raised: the route answers 500.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Raised(pub &'static str);

pub fn kyiv_today(now: DateTime<Utc>) -> NaiveDate {
    now.with_timezone(&Kyiv).date_naive()
}

fn in_python_range(date: Option<NaiveDate>) -> Result<NaiveDate, Raised> {
    date.filter(|d| (1..=9999).contains(&d.year()))
        .ok_or(Raised("date value out of range"))
}

/// `date + timedelta(days=n)`.
pub fn add_days(date: NaiveDate, days: i64) -> Result<NaiveDate, Raised> {
    let delta = TimeDelta::try_days(days).ok_or(Raised("date value out of range"))?;
    in_python_range(date.checked_add_signed(delta))
}

/// `date - relativedelta(months=n)`: the day clamped to the month's last.
fn sub_months(date: NaiveDate, months: u32) -> Result<NaiveDate, Raised> {
    in_python_range(date.checked_sub_months(Months::new(months)))
}

/// Python's truthiness of an optional string: `None` and `""` are both false.
fn non_empty(value: Option<&str>) -> Option<&str> {
    value.filter(|s| !s.is_empty())
}

fn first_of_month(date: NaiveDate) -> NaiveDate {
    date.with_day(1).expect("every month has a first day")
}

/// `parse_period(period, start_date, end_date)` with `today` already Kyiv's.
pub fn parse_period(
    period: Option<&str>,
    start_date: Option<&str>,
    end_date: Option<&str>,
    today: NaiveDate,
) -> Result<(NaiveDate, NaiveDate), Raised> {
    // `PERIOD_ALIASES`: unreachable through the route, whose validator
    // refuses both names first, and kept so this reads as the original.
    let period = period.filter(|p| !p.is_empty()).map(|p| match p {
        "thisweek" => "week",
        "thismonth" => "month",
        other => other,
    });
    let weekday = i64::from(today.weekday().num_days_from_monday());
    match period {
        Some("today") => return Ok((today, today)),
        Some("yesterday") => {
            let yesterday = add_days(today, -1)?;
            return Ok((yesterday, yesterday));
        }
        Some("week") => return Ok((add_days(today, -weekday)?, today)),
        Some("last_week") => {
            let end = add_days(add_days(today, -weekday)?, -1)?;
            return Ok((add_days(end, -6)?, end));
        }
        Some("month") => return Ok((first_of_month(today), today)),
        Some("last_month") => {
            let end = add_days(first_of_month(today), -1)?;
            return Ok((first_of_month(end), end));
        }
        Some("last_7_days") => return Ok((add_days(today, -6)?, today)),
        Some("last_28_days") => return Ok((add_days(today, -27)?, today)),
        _ => {}
    }
    if let (Some(start), Some(end)) = (non_empty(start_date), non_empty(end_date)) {
        let start = strptime_ymd(start).ok_or(Raised("start_date does not parse"))?;
        let end = strptime_ymd(end).ok_or(Raised("end_date does not parse"))?;
        return Ok((start, end));
    }
    Ok((today, today))
}

/// `_comparison_window(start, end, compare_type)`.
pub fn comparison_window(
    start: NaiveDate,
    end: NaiveDate,
    compare_type: &str,
) -> Result<(NaiveDate, NaiveDate), Raised> {
    match compare_type {
        "year_ago" => Ok((sub_months(start, 12)?, sub_months(end, 12)?)),
        "month_ago" => Ok((sub_months(start, 1)?, sub_months(end, 1)?)),
        _ => {
            // Negative when the range is reversed, and Python carries on with
            // it: the window then runs forward from the day before `start`.
            let period_days = (end - start).num_days() + 1;
            let prev_end = add_days(start, -1)?;
            Ok((add_days(prev_end, -(period_days - 1))?, prev_end))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    #[test]
    fn today_is_kyivs_whatever_utc_says() {
        // 21:30 UTC on the 20th is 00:30 on the 21st in Kyiv (UTC+3).
        let now = DateTime::parse_from_rfc3339("2026-10-20T21:30:00Z").unwrap();
        assert_eq!(kyiv_today(now.with_timezone(&Utc)), d(2026, 10, 21));
        // After the October switch Kyiv is UTC+2: 21:30 UTC is still the 25th.
        let now = DateTime::parse_from_rfc3339("2026-10-25T21:30:00Z").unwrap();
        assert_eq!(kyiv_today(now.with_timezone(&Utc)), d(2026, 10, 25));
    }

    #[test]
    fn months_back_clamp_like_relativedelta() {
        assert_eq!(
            comparison_window(d(2024, 2, 29), d(2024, 3, 31), "year_ago"),
            Ok((d(2023, 2, 28), d(2023, 3, 31)))
        );
        assert_eq!(
            comparison_window(d(2026, 3, 31), d(2026, 3, 31), "month_ago"),
            Ok((d(2026, 2, 28), d(2026, 2, 28)))
        );
    }

    #[test]
    fn python_raises_where_chrono_would_answer() {
        assert!(comparison_window(d(1, 1, 1), d(1, 1, 1), "previous_period").is_err());
        assert!(comparison_window(d(1, 6, 1), d(1, 6, 1), "year_ago").is_err());
        assert!(add_days(d(9999, 12, 31), 1).is_err());
    }

    #[test]
    fn a_reversed_range_gives_a_forward_window() {
        assert_eq!(
            comparison_window(d(2026, 1, 10), d(2026, 1, 5), "previous_period"),
            Ok((d(2026, 1, 14), d(2026, 1, 9)))
        );
    }
}
