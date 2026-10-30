//! The Rust pilot: `GET /api/revenue/trend` of the KoreanStory dashboard,
//! answer for answer with the Python app over the same Postgres Gold.
//!
//! It is not deployed and serves no one. `tests/integration/test_rust_pilot_trend.py`
//! sends both services the same requests and requires the same answers, and
//! `test_rust_pilot_access.py` does the same for who may ask.

pub mod access;
pub mod expenses;
pub mod goals;
pub mod gold;
pub mod handover;
pub mod health;
mod http;
pub mod inventory;
pub mod margin;
pub mod params;
pub mod period;
pub mod pgread;
pub mod products_intel;
pub mod pyrt;
#[rustfmt::skip]
mod pytables;
pub mod reports;
pub mod strptime;
pub mod trend;
pub mod validate;

pub use http::{AppState, router};
