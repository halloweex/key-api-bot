//! The query string, read the way Starlette and FastAPI read it.
//!
//! A repeated parameter's last value wins (`QueryParams.get`). An empty value
//! is still a value: `source_id=` is a 422, not "no source". The two typed
//! parameters go through pydantic's lax parsing, which is wider than Rust's
//! `str::parse` (`" 1"`, `"1_0"` and `"2.000"` are ints) and narrower than
//! Python's `int()` (`"1."` is not). Errors are collected in declaration
//! order, every one of them, as FastAPI reports them.

use std::collections::HashMap;

use serde::Serialize;

/// An `int` as pydantic hands it over: anything that fits in an `i64`, and
/// beyond that the digits, because Python's ints have no ceiling and a
/// 400 message quotes the number back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PyInt {
    Small(i64),
    Big(String),
}

impl PyInt {
    /// `repr(int)`: the digits, a minus sign if negative, nothing else.
    pub fn repr(&self) -> String {
        match self {
            PyInt::Small(v) => v.to_string(),
            PyInt::Big(digits) => digits.clone(),
        }
    }

    pub fn is_positive(&self) -> bool {
        match self {
            PyInt::Small(v) => *v > 0,
            PyInt::Big(digits) => !digits.starts_with('-'),
        }
    }
}

/// One entry of FastAPI's 422 `detail` list.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct FieldError {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub loc: (&'static str, &'static str),
    pub msg: &'static str,
    pub input: String,
}

/// The route's parameters after FastAPI, before the route's own validators.
#[derive(Clone, Debug, Default)]
pub struct Query {
    pub period: Option<String>,
    pub start_date: Option<String>,
    pub end_date: Option<String>,
    pub source_id: Option<PyInt>,
    pub category_id: Option<PyInt>,
    pub brand: Option<String>,
    pub promocode: Option<String>,
    /// `Query("retail")`: absent means the default, present means as sent.
    pub sales_type: String,
    /// `Query("previous_period")`.
    pub compare_type: String,
    pub include_forecast: bool,
}

/// `[+-]?[0-9]+`, normalised the way `int()` would print it.
fn plain_int(s: &str) -> Option<PyInt> {
    let (negative, digits) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let trimmed = digits.trim_start_matches('0');
    let magnitude = if trimmed.is_empty() { "0" } else { trimmed };
    let text = if negative && magnitude != "0" {
        format!("-{magnitude}")
    } else {
        magnitude.to_owned()
    };
    Some(match text.parse::<i64>() {
        Ok(v) => PyInt::Small(v),
        Err(_) => PyInt::Big(text),
    })
}

/// pydantic's lax `int` from a string. `Err` is the error type it reports.
pub fn lax_int(raw: &str) -> Result<PyInt, &'static str> {
    // pydantic-core is Rust: its trim is `str::trim`, not Python's strip.
    let s = raw.trim();
    if s.len() > 4300 {
        return Err("int_parsing_size");
    }
    if let Some(v) = plain_int(s) {
        return Ok(v);
    }
    // Underscores between digits, as in a Python literal: `1_0`, not `_1`.
    let grouped =
        (s.contains('_') && !s.starts_with('_') && !s.ends_with('_') && !s.contains("__"))
            .then(|| s.replace('_', ""));
    let s = grouped.as_deref().unwrap_or(s);
    if let Some(v) = plain_int(s) {
        return Ok(v);
    }
    // A fraction of zeros only: `2.000`, never `2.`.
    if let Some((head, fraction)) = s.split_once('.')
        && !fraction.is_empty()
        && fraction.bytes().all(|b| b == b'0')
        && let Some(v) = plain_int(head)
    {
        return Ok(v);
    }
    Err("int_parsing")
}

/// pydantic's lax `bool` from a string: case-insensitive, never trimmed.
pub fn lax_bool(raw: &str) -> Option<bool> {
    const FALSE: [&str; 6] = ["0", "f", "n", "no", "off", "false"];
    const TRUE: [&str; 6] = ["1", "t", "y", "on", "yes", "true"];
    if FALSE.iter().any(|w| raw.eq_ignore_ascii_case(w)) {
        Some(false)
    } else if TRUE.iter().any(|w| raw.eq_ignore_ascii_case(w)) {
        Some(true)
    } else {
        None
    }
}

fn int_message(kind: &str) -> &'static str {
    if kind == "int_parsing_size" {
        "Unable to parse input string as an integer, exceeded maximum size"
    } else {
        "Input should be a valid integer, unable to parse string as an integer"
    }
}

/// Parse a raw (still percent-encoded) query string.
pub fn parse(raw: &str) -> Result<Query, Vec<FieldError>> {
    let mut last: HashMap<String, String> = HashMap::new();
    for (name, value) in form_urlencoded::parse(raw.as_bytes()) {
        last.insert(name.into_owned(), value.into_owned());
    }
    let mut take = |name: &str| last.remove(name);
    let mut errors = Vec::new();

    let mut int_param = |name: &'static str, value: Option<String>| -> Option<PyInt> {
        let value = value?;
        match lax_int(&value) {
            Ok(v) => Some(v),
            Err(kind) => {
                errors.push(FieldError {
                    kind,
                    loc: ("query", name),
                    msg: int_message(kind),
                    input: value,
                });
                None
            }
        }
    };

    let period = take("period");
    let start_date = take("start_date");
    let end_date = take("end_date");
    let source_id = int_param("source_id", take("source_id"));
    let category_id = int_param("category_id", take("category_id"));
    let brand = take("brand");
    let promocode = take("promocode");
    let sales_type = take("sales_type").unwrap_or_else(|| "retail".to_owned());
    let compare_type = take("compare_type").unwrap_or_else(|| "previous_period".to_owned());
    let include_forecast = match take("include_forecast") {
        None => false,
        Some(value) => lax_bool(&value).unwrap_or_else(|| {
            errors.push(FieldError {
                kind: "bool_parsing",
                loc: ("query", "include_forecast"),
                msg: "Input should be a valid boolean, unable to interpret input",
                input: value,
            });
            false
        }),
    };

    if !errors.is_empty() {
        return Err(errors);
    }
    Ok(Query {
        period,
        start_date,
        end_date,
        source_id,
        category_id,
        brand,
        promocode,
        sales_type,
        compare_type,
        include_forecast,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_last_value_wins_and_an_empty_one_counts() {
        let q = parse("period=today&period=week&sales_type=").unwrap();
        assert_eq!(q.period.as_deref(), Some("week"));
        assert_eq!(q.sales_type, "");
        assert_eq!(q.compare_type, "previous_period");
    }

    #[test]
    fn every_bad_typed_parameter_is_reported_in_declaration_order() {
        let errors = parse("include_forecast=maybe&category_id=y&source_id=").unwrap_err();
        let names: Vec<_> = errors.iter().map(|e| e.loc.1).collect();
        assert_eq!(names, ["source_id", "category_id", "include_forecast"]);
        assert_eq!(
            serde_json::to_string(&errors[0]).unwrap(),
            r#"{"type":"int_parsing","loc":["query","source_id"],"msg":"Input should be a valid integer, unable to parse string as an integer","input":""}"#
        );
    }
}
