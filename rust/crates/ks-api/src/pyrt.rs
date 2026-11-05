//! The parts of CPython's runtime the trend payload depends on.
//!
//! Porting the endpoint turned out to be mostly porting these, not the SQL:
//! which zeros are `int` and which are `float`, what `round()` and `sum()`
//! return, how `repr()` quotes a string inside a 400, what `str.strip()`
//! calls whitespace. Each is pinned against CPython 3.14 by
//! `tests/python_semantics.rs`, from fixtures `gen_pytables.py` writes.

use std::fmt::Write as _;

use serde::{Serialize, Serializer};

use crate::pytables::{NOT_PRINTABLE, WHITESPACE};

/// A number as the Python code left it. A day with no Gold row is the `int`
/// `0`, a day whose row says ₴0.00 is the `float` `0.0`, and the JSON says
/// which (`0` against `0.0`). Collapsing the two into `f64` would change the
/// bytes of nearly every response.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PyNum {
    Int(i64),
    Float(f64),
}

impl PyNum {
    pub fn as_f64(self) -> f64 {
        match self {
            // The payload's ints are day counts and zeros; exact in f64.
            PyNum::Int(i) => i as f64,
            PyNum::Float(f) => f,
        }
    }
}

impl Serialize for PyNum {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match *self {
            PyNum::Int(i) => s.serialize_i64(i),
            PyNum::Float(f) => s.serialize_f64(f),
        }
    }
}

/// `round(x, ndigits)` for a float: the double nearest the decimal that the
/// exact binary value rounds to, ties to even (`double_round`, `_Py_dg_dtoa`
/// mode 3). Rust's fixed-precision `Display` rounds the exact value the same
/// way, so formatting and reading back is CPython's algorithm, not an
/// approximation of it — `(x * 100.0).round() / 100.0` is the approximation,
/// and it disagrees on ties (`round(0.125, 2)` is `0.12`).
pub fn round_float(x: f64, ndigits: usize) -> f64 {
    if !x.is_finite() {
        return x;
    }
    format!("{x:.ndigits$}")
        .parse()
        .expect("a formatted finite float parses back")
}

/// `round(n, ndigits)`: an `int` comes back unchanged, still an `int`.
pub fn round_num(n: PyNum, ndigits: usize) -> PyNum {
    match n {
        PyNum::Int(_) => n,
        PyNum::Float(f) => PyNum::Float(round_float(f, ndigits)),
    }
}

/// Builtin `sum(items)` with its start of `0`, as CPython 3.12+ computes it:
/// an `int` while every item is one, then from the first float on a
/// Neumaier-compensated float sum. Not `iter().sum::<f64>()`: compensation
/// moves the last bits, and the last bits are what `round(total, 2)` sees.
pub fn py_sum(items: &[PyNum]) -> PyNum {
    let mut rest = items.iter().copied();
    let mut int_total: i64 = 0;
    let mut hi = None;
    for item in rest.by_ref() {
        match item {
            PyNum::Int(i) => {
                int_total = int_total
                    .checked_add(i)
                    .expect("the payload's ints are zeros and day counts");
            }
            PyNum::Float(f) => {
                // `int + float` is one plain addition; the float loop starts here.
                hi = Some(int_total as f64 + f);
                break;
            }
        }
    }
    let Some(mut hi) = hi else {
        return PyNum::Int(int_total);
    };
    let mut lo = 0.0_f64;
    for item in rest {
        let x = item.as_f64();
        let t = hi + x;
        if hi.abs() >= x.abs() {
            lo += (hi - t) + x;
        } else {
            lo += (x - t) + hi;
        }
        hi = t;
    }
    // CPython adds the compensation only when it is a finite non-zero, so a
    // sum that overflowed stays infinite rather than becoming NaN.
    PyNum::Float(if lo != 0.0 && lo.is_finite() {
        hi + lo
    } else {
        hi
    })
}

/// `str.isspace()`: what `str.strip()` removes. Wider than `char::is_whitespace`
/// — U+001C..U+001F are whitespace to Python and not to Unicode.
pub fn py_isspace(c: char) -> bool {
    WHITESPACE.binary_search(&(c as u32)).is_ok()
}

/// `str.strip()` with no argument.
pub fn py_strip(s: &str) -> &str {
    s.trim_matches(py_isspace)
}

/// `value.lower().strip()` — the validators' normalisation, in their order.
pub fn py_lower_strip(s: &str) -> String {
    py_strip(&s.to_lowercase()).to_owned()
}

fn py_isprintable(c: char) -> bool {
    let cp = c as u32;
    if cp < 0x80 {
        return (0x20..0x7f).contains(&cp);
    }
    // Ranges are sorted and disjoint: find the last one starting at or before.
    let i = NOT_PRINTABLE.partition_point(|&(start, _)| start <= cp);
    i == 0 || NOT_PRINTABLE[i - 1].1 < cp
}

/// `repr(s)` for a `str`, as the 400 messages carry it: `(got: 'xyz')`.
/// Single quotes unless the text holds one and no double quote.
pub fn repr_str(s: &str) -> String {
    let quote = if s.contains('\'') && !s.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::with_capacity(s.len() + 2);
    out.push(quote);
    for c in s.chars() {
        let cp = c as u32;
        match c {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ if c == quote => {
                out.push('\\');
                out.push(c);
            }
            _ if cp < 0x20 || cp == 0x7f => {
                let _ = write!(out, "\\x{cp:02x}");
            }
            _ if cp < 0x7f || py_isprintable(c) => out.push(c),
            _ if cp <= 0xff => {
                let _ = write!(out, "\\x{cp:02x}");
            }
            _ if cp <= 0xffff => {
                let _ = write!(out, "\\u{cp:04x}");
            }
            _ => {
                let _ = write!(out, "\\U{cp:08x}");
            }
        }
    }
    out.push(quote);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_day_with_no_row_stays_an_int_through_round_and_sum() {
        assert_eq!(round_num(PyNum::Int(0), 2), PyNum::Int(0));
        assert_eq!(py_sum(&[PyNum::Int(0), PyNum::Int(0)]), PyNum::Int(0));
        assert_eq!(py_sum(&[]), PyNum::Int(0));
        assert_eq!(
            serde_json::to_string(&[PyNum::Int(0), PyNum::Float(0.0)]).unwrap(),
            "[0,0.0]"
        );
    }

    #[test]
    fn round_breaks_ties_to_even_on_the_exact_value() {
        assert_eq!(round_float(0.125, 2), 0.12);
        assert_eq!(round_float(0.375, 2), 0.38);
        // 2.675 is 2.67499999... in binary: CPython says 2.67, the naive
        // scale-and-round says 2.68.
        assert_eq!(round_float(2.675, 2), 2.67);
        assert_eq!((2.675_f64 * 100.0).round() / 100.0, 2.68);
    }

    #[test]
    fn compensation_is_why_the_sum_is_not_a_fold() {
        let items: Vec<PyNum> = [1e16, 1.0, -1e16].into_iter().map(PyNum::Float).collect();
        assert_eq!(py_sum(&items), PyNum::Float(1.0));
        assert_eq!(1e16_f64 + 1.0 - 1e16, 0.0);
    }

    #[test]
    fn strip_takes_the_separators_rust_leaves() {
        assert_eq!(py_strip("\u{1f}today\u{1c}"), "today");
        assert_eq!("\u{1f}today\u{1c}".trim(), "\u{1f}today\u{1c}");
    }
}
