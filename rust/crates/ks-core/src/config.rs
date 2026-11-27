//! Environment variables, read the way the Python app reads them.
//!
//! The core shares the Python app's names wherever a name means the same
//! thing, so one `.env` configures both and a value cannot mean one thing to
//! one process and another to the other.

use std::fmt;
use std::time::Duration;

/// The value with surrounding whitespace removed; `None` when unset, blank or
/// not Unicode — `os.getenv(name, "").strip()` and its falsy empty string.
pub fn trimmed(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

/// The value exactly as given; `None` when unset or empty —
/// `os.getenv(name, "")` for a value whose whitespace belongs to it, such as
/// a password.
pub fn raw(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// A setting that cannot be used. Its text names the variable, never its value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

/// `float(os.getenv(name, default))` seconds, as `core/pg.py` reads
/// `KS_PG_TIMEOUT`. A value that is not a positive number is an error rather
/// than the default: a timeout somebody set and the process ignored is the
/// worse failure.
pub fn seconds(name: &str, default: f64) -> Result<Duration, ConfigError> {
    let value = match trimmed(name) {
        None => default,
        Some(text) => text
            .parse::<f64>()
            .map_err(|_| ConfigError(format!("{name} is not a number of seconds")))?,
    };
    if !value.is_finite() || value <= 0.0 {
        return Err(ConfigError(format!(
            "{name} must be a positive number of seconds"
        )));
    }
    Ok(Duration::from_secs_f64(value))
}

/// `int(os.getenv(name, default))`, as `core/pg.py` reads the pool sizes.
pub fn count(name: &str, default: u32) -> Result<u32, ConfigError> {
    match trimmed(name) {
        None => Ok(default),
        Some(text) => text
            .parse::<u32>()
            .map_err(|_| ConfigError(format!("{name} is not a whole number"))),
    }
}
