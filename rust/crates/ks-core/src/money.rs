//! Hryvnia as a decimal, never a float (plan, revision 4: «деньги Decimal»).
//!
//! Every amount the core moves comes out of a `NUMERIC` column and goes into a
//! ClickHouse `Decimal`, and on the way it is text: the text Python's
//! `str(Decimal)` writes. `rust_decimal` keeps the column's scale — a
//! `NUMERIC(12,2)` arrives as `1234.50`, not `1234.5` — so its `Display` is
//! that text. Equality is numeric, as `Decimal`'s is in Python: `100` read
//! back from ClickHouse equals `100.00` sent to it.
//!
//! There is no conversion from or to `f64` here, on purpose.

use std::fmt;
use std::str::FromStr;

use rust_decimal::Decimal;

/// An amount of money, exact.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Money(Decimal);

impl Money {
    /// The amount a `NUMERIC` column held, scale and all.
    pub fn new(value: Decimal) -> Self {
        Money(value)
    }

    /// The decimal itself.
    pub fn as_decimal(self) -> Decimal {
        self.0
    }

    /// `abs(a - b)`, exactly.
    pub fn abs_diff(self, other: Money) -> Money {
        Money((self.0 - other.0).abs())
    }

    /// One kopeck: the allowance `compare_gold_cells` gives `avg_order_value`,
    /// because Postgres and ClickHouse divide decimals differently — Postgres
    /// rounds into `NUMERIC(12,2)`, ClickHouse truncates at the scale.
    pub fn kopeck() -> Money {
        Money(Decimal::new(1, 2))
    }
}

impl fmt::Display for Money {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

/// Text that is not an amount.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MoneyError(pub String);

impl fmt::Display for MoneyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for MoneyError {}

impl FromStr for Money {
    type Err = MoneyError;

    /// Exact or refused: a figure with more digits than a decimal holds is
    /// an error, never a rounding.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Decimal::from_str_exact(text)
            .map(Money)
            .map_err(|e| MoneyError(format!("{text:?} is not an amount: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_columns_scale_is_the_text() {
        // asyncpg hands Python Decimal('1234.50') for NUMERIC(12,2), and
        // str() writes it back with both places.
        assert_eq!(Money::new(Decimal::new(123_450, 2)).to_string(), "1234.50");
        assert_eq!(Money::new(Decimal::new(0, 2)).to_string(), "0.00");
        assert_eq!(Money::new(Decimal::new(-1_250, 2)).to_string(), "-12.50");
        assert_eq!(
            Money::new(Decimal::new(9_999_999_999, 2)).to_string(),
            "99999999.99"
        );
    }

    #[test]
    fn equality_is_numeric_as_in_python() {
        let sent: Money = "100.00".parse().unwrap();
        let read_back: Money = "100".parse().unwrap();
        assert_eq!(sent, read_back);
    }

    #[test]
    fn the_kopeck_is_exact() {
        let a: Money = "123.45".parse().unwrap();
        let b: Money = "123.44".parse().unwrap();
        assert_eq!(a.abs_diff(b), Money::kopeck());
        assert_eq!(b.abs_diff(a), Money::kopeck());
        assert!("123.43".parse::<Money>().unwrap().abs_diff(a) > Money::kopeck());
    }

    #[test]
    fn a_figure_too_long_is_refused_not_rounded() {
        assert!("1.00000000000000000000000000001".parse::<Money>().is_err());
        assert!("NaN".parse::<Money>().is_err());
        assert!("".parse::<Money>().is_err());
    }
}
