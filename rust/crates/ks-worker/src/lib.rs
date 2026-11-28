//! The core's jobs in Rust (plan, revision 4). К1 ports one: [`ch_sync`],
//! the hourly ClickHouse copy. К2 ports the load from KeyCRM one table at a
//! time, into a shadow beside each ([`landing`]). `src/main.rs` is the
//! process that runs them.

pub mod ch_sync;
pub mod landing;
