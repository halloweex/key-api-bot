//! The route's validators (`core/validators.py`), in the route's order and
//! with their messages to the character: a 400's `detail` is what the
//! frontend shows and what a script greps.
//!
//! Two of the route's habits are kept on purpose. It calls
//! `validate_period` and throws the result away, so `TODAY` passes the
//! check and then matches no shortcut — `parse_period` sees the raw value.
//! And `validate_promocode` strips a blank promocode to `""`, which is
//! falsy, so `promocode=%20` is no promocode at all.

use crate::params::PyInt;
use crate::pyrt::{py_isspace, py_lower_strip, py_strip, repr_str};

const PERIODS: [&str; 8] = [
    "today",
    "yesterday",
    "week",
    "last_week",
    "month",
    "last_month",
    "last_7_days",
    "last_28_days",
];
/// `KNOWN_SALES_TYPES | {"all"}` (`core/duckdb_constants.py`).
const SALES_TYPES: [&str; 5] = ["retail", "b2b", "internal", "exhibition", "all"];
const MAX_BRAND_LENGTH: usize = 255;
const MAX_PROMOCODE_LENGTH: usize = 100;

/// `', '.join(sorted(values))`.
fn sorted_list(values: &[&str]) -> String {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted.join(", ")
}

/// `validate_period`: only the error matters, the route ignores the value.
pub fn period(value: Option<&str>) -> Result<(), String> {
    let Some(value) = value.filter(|v| !v.is_empty()) else {
        return Ok(());
    };
    let value = py_lower_strip(value);
    if PERIODS.contains(&value.as_str()) {
        return Ok(());
    }
    Err(format!(
        "period: Must be one of: {} (got: {})",
        sorted_list(&PERIODS),
        repr_str(&value)
    ))
}

/// `validate_source_id` against `VALID_SOURCE_IDS = {1, 2, 3, 4}`.
pub fn source_id(value: Option<&PyInt>) -> Result<Option<i32>, String> {
    match value {
        None => Ok(None),
        Some(PyInt::Small(v @ 1..=4)) => Ok(i32::try_from(*v).ok()),
        Some(other) => Err(format!(
            "source_id: Must be one of [1, 2, 3, 4] (got: {})",
            other.repr()
        )),
    }
}

/// `validate_category_id`; `Ok(true)` when a category was asked for.
pub fn category_id(value: Option<&PyInt>) -> Result<bool, String> {
    match value {
        None => Ok(false),
        Some(v) if !v.is_positive() => Err(format!(
            "category_id: Must be a positive integer (got: {})",
            v.repr()
        )),
        // `bronze.categories.id` is an integer column.
        Some(PyInt::Small(n)) if *n <= INT32_MAX => Ok(true),
        Some(v) => Err(format!(
            "category_id: Must be at most {INT32_MAX} (got: {})",
            v.repr()
        )),
    }
}

/// `INT32_MAX` in `core/validators.py`: the widest `integer` column.
pub const INT32_MAX: i64 = i32::MAX as i64;

/// `validate_db_int(value, field, bits)`: an integer held to the width of
/// the column it meets; `None` when it fits.
pub fn db_int(value: Option<&PyInt>, field: &str, bits: u32) -> Option<String> {
    let (low, high): (i128, i128) = if bits == 64 {
        (i64::MIN.into(), i64::MAX.into())
    } else {
        (i32::MIN.into(), i32::MAX.into())
    };
    let fits = match value? {
        PyInt::Small(n) => (low..=high).contains(&i128::from(*n)),
        PyInt::Big(_) => false,
    };
    (!fits).then(|| {
        format!(
            "{field}: Must be between {low} and {high} (got: {})",
            value.map(PyInt::repr).unwrap_or_default()
        )
    })
}

/// Python's `\w` approximated by `char::is_alphanumeric`; they part on
/// Unicode's Other_Alphabetic marks (a Devanagari vowel sign is a letter
/// here and not there). A brand only ever selects the line-level branch,
/// which the pilot does not serve, so the difference is 400 against 501.
fn brand_char(c: char) -> bool {
    c == '_' || c.is_alphanumeric() || py_isspace(c) || "-.'&,()".contains(c)
}

/// `validate_brand_name`; `Ok(true)` when a brand was asked for.
pub fn brand(value: Option<&str>) -> Result<bool, String> {
    let Some(value) = value.filter(|v| !v.is_empty()) else {
        return Ok(false);
    };
    let value = py_strip(value);
    let length = value.chars().count();
    if length > MAX_BRAND_LENGTH {
        return Err(format!(
            "brand: Cannot exceed {MAX_BRAND_LENGTH} characters (got: {})",
            repr_str(&format!("{length} characters"))
        ));
    }
    if value.is_empty() || !value.chars().all(brand_char) {
        return Err(format!(
            "brand: Contains invalid characters (got: {})",
            repr_str(value)
        ));
    }
    Ok(true)
}

/// `validate_promocode`; `Ok(true)` when the stripped code is not empty.
pub fn promocode(value: Option<&str>) -> Result<bool, String> {
    let Some(value) = value.filter(|v| !v.is_empty()) else {
        return Ok(false);
    };
    let value = py_strip(value);
    if value.chars().count() > MAX_PROMOCODE_LENGTH {
        return Err(format!(
            "promocode: Promocode too long (max {MAX_PROMOCODE_LENGTH} characters) (got: {})",
            repr_str(value)
        ));
    }
    Ok(!value.is_empty())
}

/// `validate_sales_type`: the normalised value, `retail` for an empty one.
pub fn sales_type(value: &str) -> Result<String, String> {
    if value.is_empty() {
        return Ok("retail".to_owned());
    }
    let value = py_lower_strip(value);
    if SALES_TYPES.contains(&value.as_str()) {
        return Ok(value);
    }
    Err(format!(
        "sales_type: Must be one of: {} (got: {})",
        sorted_list(&SALES_TYPES),
        repr_str(&value)
    ))
}

/// The route's own rule: anything unrecognised is `previous_period`,
/// compared as sent — `YEAR_AGO` is not `year_ago`.
pub fn compare_type(value: &str) -> &str {
    match value {
        "previous_period" | "year_ago" | "month_ago" => value,
        _ => "previous_period",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn messages_are_the_python_ones() {
        assert_eq!(
            period(Some("xyz")).unwrap_err(),
            "period: Must be one of: last_28_days, last_7_days, last_month, last_week, \
             month, today, week, yesterday (got: 'xyz')"
        );
        assert_eq!(
            sales_type("WHOLESALE").unwrap_err(),
            "sales_type: Must be one of: all, b2b, exhibition, internal, retail (got: 'wholesale')"
        );
        assert_eq!(
            source_id(Some(&PyInt::Small(5))).unwrap_err(),
            "source_id: Must be one of [1, 2, 3, 4] (got: 5)"
        );
        assert_eq!(
            brand(Some("  ")).unwrap_err(),
            "brand: Contains invalid characters (got: '')"
        );
    }

    #[test]
    fn the_quirks_the_route_has() {
        assert_eq!(period(Some("TODAY")), Ok(()));
        assert_eq!(promocode(Some("   ")), Ok(false));
        assert_eq!(sales_type(" B2B "), Ok("b2b".to_owned()));
        assert_eq!(compare_type("YEAR_AGO"), "previous_period");
    }
}
