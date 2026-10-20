//! `datetime.strptime(s, "%Y-%m-%d")`, the one format the endpoint parses.
//!
//! CPython compiles the format into a regex and `match`es it from the start:
//!
//! ```text
//! (?P<Y>\d\d\d\d)-(?P<m>1[0-2]|0[1-9]|[1-9])-(?P<d>3[01]|[12]\d|0[1-9]|[1-9]| [1-9])
//! ```
//!
//! then refuses a match shorter than the input ("unconverted data remains").
//! Each group takes the first alternative that fits — a literal `-` or the
//! end of the pattern follows every group, so backtracking never helps.
//! `\d` is Unicode (`str.isdecimal`) while the bracket classes are ASCII,
//! which is why `２０２６-01-01` is a date and `2026-1٥-01` is not.

use chrono::NaiveDate;

use crate::pytables::DECIMAL_ZEROS;

/// The value of a decimal digit in any script, as `int()` reads it.
pub fn decimal_value(c: char) -> Option<u32> {
    let cp = c as u32;
    let i = DECIMAL_ZEROS.partition_point(|&zero| zero <= cp);
    let zero = *DECIMAL_ZEROS.get(i.checked_sub(1)?)?;
    (cp - zero < 10).then_some(cp - zero)
}

fn ascii(c: char) -> u32 {
    c as u32 - '0' as u32
}

/// The date `strptime` returns, or `None` where it raises `ValueError`.
pub fn strptime_ymd(s: &str) -> Option<NaiveDate> {
    let chars: Vec<char> = s.chars().collect();
    let at = |i: usize| chars.get(i).copied();

    let mut year = 0;
    for i in 0..4 {
        year = year * 10 + decimal_value(at(i)?)?;
    }
    let mut pos = 4;
    if at(pos)? != '-' {
        return None;
    }
    pos += 1;

    let (month, width) = match (at(pos)?, at(pos + 1)) {
        ('1', Some(d @ '0'..='2')) => (10 + ascii(d), 2),
        ('0', Some(d @ '1'..='9')) => (ascii(d), 2),
        (d @ '1'..='9', _) => (ascii(d), 1),
        _ => return None,
    };
    pos += width;
    if at(pos)? != '-' {
        return None;
    }
    pos += 1;

    let (first, next) = (at(pos)?, at(pos + 1));
    let (day, width) = match first {
        '3' if matches!(next, Some('0' | '1')) => (30 + ascii(next?), 2),
        '1' | '2' if next.and_then(decimal_value).is_some() => {
            (ascii(first) * 10 + decimal_value(next?)?, 2)
        }
        '0' if matches!(next, Some('1'..='9')) => (ascii(next?), 2),
        '1'..='9' => (ascii(first), 1),
        ' ' if matches!(next, Some('1'..='9')) => (ascii(next?), 2),
        _ => return None,
    };
    pos += width;

    if pos != chars.len() || year == 0 {
        // "unconverted data remains", or "year must be in 1..9999".
        return None;
    }
    // `None` for 30 February: "day 30 must be in range 1..28".
    NaiveDate::from_ymd_opt(i32::try_from(year).ok()?, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ymd(y: i32, m: u32, d: u32) -> Option<NaiveDate> {
        NaiveDate::from_ymd_opt(y, m, d)
    }

    #[test]
    fn the_padding_python_forgives() {
        assert_eq!(strptime_ymd("2026-1-5"), ymd(2026, 1, 5));
        assert_eq!(strptime_ymd("2026-01- 5"), ymd(2026, 1, 5));
    }

    #[test]
    fn the_first_alternative_wins_and_the_rest_is_unconverted() {
        assert_eq!(strptime_ymd("2026-01-3"), ymd(2026, 1, 3));
        assert_eq!(strptime_ymd("2026-01-310"), None);
        assert_eq!(strptime_ymd("2026-01-01 "), None);
    }

    #[test]
    fn unicode_digits_count_only_where_the_pattern_says_backslash_d() {
        assert_eq!(strptime_ymd("２０２６-01-01"), ymd(2026, 1, 1));
        assert_eq!(strptime_ymd("2026-01-1٥"), ymd(2026, 1, 15));
        assert_eq!(strptime_ymd("2026-1٥-01"), None);
    }
}
