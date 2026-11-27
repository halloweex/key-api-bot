//! The Kyiv calendar date of an instant.
//!
//! Every date in the warehouse is Kyiv's: Silver's `order_date` is
//! `(ordered_at AT TIME ZONE 'Europe/Kyiv')::date`, rendered by
//! `core/sql_dialect.py`, and "today" is Kyiv's whatever the process's own
//! zone says — production's containers run `TZ=Europe/Kyiv`, CI runs UTC.
//! The answer is read off the instant through the tz database, never off a
//! fixed offset: the clock-change days are 23 and 25 hours long.
//!
//! The ClickHouse copy computes no date — Postgres has, by the time Silver
//! is read — but the shadow's comparison is once a Kyiv day, and every later
//! component (the derivation, the trend) asks this question. The tests hold
//! it to CPython's `zoneinfo` answers (`contract/ch_sync.json`, `kyiv_dates`).

use chrono::{DateTime, NaiveDate, Utc};
use chrono_tz::Europe::Kyiv;

/// The Kyiv date of `instant`.
pub fn date_of(instant: DateTime<Utc>) -> NaiveDate {
    instant.with_timezone(&Kyiv).date_naive()
}

/// Today in Kyiv.
pub fn today() -> NaiveDate {
    date_of(Utc::now())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .unwrap()
            .with_timezone(&Utc)
    }

    fn day(text: &str) -> NaiveDate {
        NaiveDate::parse_from_str(text, "%Y-%m-%d").unwrap()
    }

    #[test]
    fn a_second_either_side_of_the_midnights_that_matter() {
        // tests/rust_pilot_fixture.py's BOUNDARY_ORDERS: EEST is UTC+3, EET UTC+2.
        for (instant, expected) in [
            ("2025-10-25T20:59:59+00:00", "2025-10-25"),
            ("2025-10-25T21:00:00+00:00", "2025-10-26"),
            ("2025-10-26T21:59:59+00:00", "2025-10-26"),
            ("2025-10-26T22:00:00+00:00", "2025-10-27"),
            ("2026-03-28T21:59:59+00:00", "2026-03-28"),
            ("2026-03-28T22:00:00+00:00", "2026-03-29"),
            ("2026-03-29T20:59:59+00:00", "2026-03-29"),
            ("2026-03-29T21:00:00+00:00", "2026-03-30"),
            ("2026-12-31T21:59:59+00:00", "2026-12-31"),
            ("2026-12-31T22:00:00+00:00", "2027-01-01"),
        ] {
            assert_eq!(date_of(at(instant)), day(expected), "{instant}");
        }
    }
}
