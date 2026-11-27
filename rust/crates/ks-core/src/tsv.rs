//! ClickHouse's TabSeparated, both directions — `core/ch_common.py`'s
//! `render_tsv` and `parse_tsv`, byte for byte, typed per table by the
//! table's own column list.
//!
//! The escaping is ClickHouse's own and is written for data that never needs
//! it: a format that is correct only for today's values breaks silently.
//! ClickHouse writes `\b`, `\f` and `\0` for backspace, form feed and NUL,
//! and the Python side once read `\b` back as a plain `b` (L2-12) — a
//! promocode with a control character came back from the round trip as
//! another string, and the zero-tolerance comparison blamed the copy.

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

use crate::money::Money;

/// A column's wire type: the values of `core/ch_common.py`'s type maps, with
/// Python's `int` split by the Postgres width it is read from, because sqlx
/// decodes an `INTEGER` and a `BIGINT` as different types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// `int`, from a Postgres `INTEGER`.
    Int32,
    /// `int`, from a Postgres `BIGINT`.
    Int64,
    /// `decimal`, from a `NUMERIC`.
    Decimal,
    /// `str`.
    Str,
    /// `date`.
    Date,
    /// `bool`.
    Bool,
    /// `ts`: ClickHouse `DateTime('UTC')`, whole seconds.
    Ts,
    /// `ts6`: ClickHouse `DateTime64(6, 'UTC')`, microseconds.
    Ts6,
}

impl Kind {
    /// The key Python's type maps give this kind.
    pub fn python_key(self) -> &'static str {
        match self {
            Kind::Int32 | Kind::Int64 => "int",
            Kind::Decimal => "decimal",
            Kind::Str => "str",
            Kind::Date => "date",
            Kind::Bool => "bool",
            Kind::Ts => "ts",
            Kind::Ts6 => "ts6",
        }
    }
}

/// A table's column: its name and wire type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Column {
    pub name: &'static str,
    pub kind: Kind,
}

/// `name, name, …` — the column list every statement spells.
pub fn names(columns: &[Column]) -> String {
    columns
        .iter()
        .map(|column| column.name)
        .collect::<Vec<_>>()
        .join(", ")
}

/// One value. `Time` is either timestamp kind; the column says which.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cell {
    Null,
    Int(i64),
    Money(Money),
    Str(String),
    Date(NaiveDate),
    Bool(bool),
    Time(DateTime<Utc>),
}

/// A row that does not fit its columns, or text that is not a row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TsvError(pub String);

impl std::fmt::Display for TsvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TsvError {}

/// ClickHouse's escapes in `core/ch_common.py`'s order (`_ESCAPES`), the
/// backslash first. Python applies them one `str.replace` after another; no
/// replacement writes a character a later one looks for, so one pass over
/// the characters writes the same bytes.
pub const ESCAPES: [(char, &str); 7] = [
    ('\\', "\\\\"),
    ('\t', "\\t"),
    ('\n', "\\n"),
    ('\r', "\\r"),
    ('\u{8}', "\\b"),
    ('\u{c}', "\\f"),
    ('\0', "\\0"),
];

/// `core.ch_common.escape`, appended to `out`.
pub fn escape_into(value: &str, out: &mut String) {
    for ch in value.chars() {
        match ESCAPES.iter().find(|(raw, _)| *raw == ch) {
            Some((_, escaped)) => out.push_str(escaped),
            None => out.push(ch),
        }
    }
}

/// `core.ch_common.escape`.
pub fn escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    escape_into(value, &mut out);
    out
}

/// `core.ch_common.unescape`: a backslash and the character after it. A
/// sequence it does not know is that character (`\'` is `'`); a backslash
/// with nothing after it is kept.
pub fn unescape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut chars = value.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\'
            && let Some(next) = chars.next()
        {
            out.push(match next {
                't' => '\t',
                'n' => '\n',
                'r' => '\r',
                'b' => '\u{8}',
                'f' => '\u{c}',
                '0' => '\0',
                other => other,
            });
            continue;
        }
        out.push(ch);
    }
    out
}

/// `core.ch_common.render_tsv`: one line per row, each ending in a newline,
/// `\N` for NULL, bools as `true`/`false`, timestamps in UTC.
pub fn render(rows: &[Vec<Cell>], columns: &[Column]) -> Result<Vec<u8>, TsvError> {
    let mut text = String::new();
    for row in rows {
        for (i, (column, cell)) in columns.iter().zip(row).enumerate() {
            if i > 0 {
                text.push('\t');
            }
            render_cell(column, cell, &mut text)?;
        }
        text.push('\n');
    }
    Ok(text.into_bytes())
}

fn render_cell(column: &Column, cell: &Cell, out: &mut String) -> Result<(), TsvError> {
    match (column.kind, cell) {
        (_, Cell::Null) => out.push_str("\\N"),
        (Kind::Bool, Cell::Bool(value)) => out.push_str(if *value { "true" } else { "false" }),
        // `strftime("%Y-%m-%d %H:%M:%S")`: the fraction is dropped, not rounded.
        (Kind::Ts, Cell::Time(at)) => out.push_str(&at.format("%Y-%m-%d %H:%M:%S").to_string()),
        (Kind::Ts6, Cell::Time(at)) => {
            out.push_str(&at.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
        }
        (Kind::Date, Cell::Date(day)) => out.push_str(&day.format("%Y-%m-%d").to_string()),
        (Kind::Str, Cell::Str(value)) => escape_into(value, out),
        (Kind::Int32 | Kind::Int64, Cell::Int(value)) => out.push_str(&value.to_string()),
        (Kind::Decimal, Cell::Money(value)) => out.push_str(&value.to_string()),
        (kind, cell) => {
            return Err(TsvError(format!(
                "column {} is {kind:?}, and its value is {cell:?}",
                column.name
            )));
        }
    }
    Ok(())
}

/// `core.ch_common.parse_tsv`: what ClickHouse answers `FORMAT TabSeparated`
/// with, typed by `columns`. Empty lines are skipped, as Python skips them.
pub fn parse(text: &str, columns: &[Column]) -> Result<Vec<Vec<Cell>>, TsvError> {
    let mut rows = Vec::new();
    for line in text.split('\n') {
        if line.is_empty() {
            continue;
        }
        let row = columns
            .iter()
            .zip(line.split('\t'))
            .map(|(column, cell)| parse_cell(column, cell))
            .collect::<Result<Vec<_>, _>>()?;
        rows.push(row);
    }
    Ok(rows)
}

fn parse_cell(column: &Column, cell: &str) -> Result<Cell, TsvError> {
    if cell == "\\N" {
        return Ok(Cell::Null);
    }
    let refused = |what: &str| TsvError(format!("column {}: {cell:?} is not {what}", column.name));
    Ok(match column.kind {
        Kind::Bool => Cell::Bool(cell == "true"),
        Kind::Ts => Cell::Time(
            NaiveDateTime::parse_from_str(cell, "%Y-%m-%d %H:%M:%S")
                .map_err(|_| refused("a DateTime"))?
                .and_utc(),
        ),
        Kind::Ts6 => Cell::Time(
            NaiveDateTime::parse_from_str(cell, "%Y-%m-%d %H:%M:%S%.f")
                .map_err(|_| refused("a DateTime64"))?
                .and_utc(),
        ),
        Kind::Date => {
            Cell::Date(NaiveDate::parse_from_str(cell, "%Y-%m-%d").map_err(|_| refused("a Date"))?)
        }
        Kind::Decimal => Cell::Money(cell.parse().map_err(|_| refused("a Decimal"))?),
        Kind::Int32 | Kind::Int64 => Cell::Int(cell.parse().map_err(|_| refused("an integer"))?),
        Kind::Str => Cell::Str(unescape(cell)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const COLUMNS: [Column; 4] = [
        Column {
            name: "id",
            kind: Kind::Int32,
        },
        Column {
            name: "code",
            kind: Kind::Str,
        },
        Column {
            name: "total",
            kind: Kind::Decimal,
        },
        Column {
            name: "ok",
            kind: Kind::Bool,
        },
    ];

    #[test]
    fn every_escape_goes_both_ways() {
        let nasty = "a\\b\tc\nd\re\u{8}f\u{c}g\0h \\N кириллица 🎁 '";
        let escaped = escape(nasty);
        assert_eq!(
            escaped,
            "a\\\\b\\tc\\nd\\re\\bf\\fg\\0h \\\\N кириллица 🎁 '"
        );
        assert_eq!(unescape(&escaped), nasty);
    }

    #[test]
    fn an_unknown_escape_is_its_character_and_a_trailing_backslash_stays() {
        assert_eq!(unescape("it\\'s"), "it's");
        assert_eq!(unescape("end\\"), "end\\");
    }

    #[test]
    fn rows_end_in_newlines_and_nothing_is_empty() {
        let rows = vec![
            vec![
                Cell::Int(1),
                Cell::Str("x\ty".into()),
                Cell::Money("12.50".parse().unwrap()),
                Cell::Bool(true),
            ],
            vec![Cell::Int(2), Cell::Null, Cell::Null, Cell::Bool(false)],
        ];
        let text = render(&rows, &COLUMNS).unwrap();
        assert_eq!(text, b"1\tx\\ty\t12.50\ttrue\n2\t\\N\t\\N\tfalse\n");
        assert_eq!(render(&[], &COLUMNS).unwrap(), b"");
        assert_eq!(
            parse(std::str::from_utf8(&text).unwrap(), &COLUMNS).unwrap(),
            rows
        );
    }

    #[test]
    fn a_value_of_the_wrong_kind_is_refused() {
        let rows = vec![vec![Cell::Str("1".into())]];
        assert!(render(&rows, &COLUMNS[..1]).is_err());
    }

    #[test]
    fn timestamps_drop_the_fraction_or_keep_six_digits() {
        let at = DateTime::parse_from_rfc3339("2026-03-28T21:59:59.999999+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let ts = [Column {
            name: "t",
            kind: Kind::Ts,
        }];
        let ts6 = [Column {
            name: "t",
            kind: Kind::Ts6,
        }];
        let row = vec![vec![Cell::Time(at)]];
        assert_eq!(render(&row, &ts).unwrap(), b"2026-03-28 21:59:59\n");
        assert_eq!(render(&row, &ts6).unwrap(), b"2026-03-28 21:59:59.999999\n");
        assert_eq!(parse("2026-03-28 21:59:59.999999\n", &ts6).unwrap(), row);
    }
}
