//! The core's jobs in Rust (plan, revision 4). К1 ports one: [`ch_sync`],
//! the hourly ClickHouse copy. `src/main.rs` is the process that runs it.

pub mod ch_sync;
