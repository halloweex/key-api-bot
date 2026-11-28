//! К2, the load from KeyCRM (the plan, revision 4): each landing table's
//! parse and write, ported one table at a time, cheapest first. Rust writes
//! only the shadow beside each table; the live tables stay Python's until
//! the owner says yes to a switch.
//!
//! Step 1 ports the first table, chain 6a's expense-type dictionary
//! ([`expense_types`]), and proves it against Python on the same payloads:
//! `contract/expense_types.json` for the parse, and
//! `tests/integration/test_rust_k2_expense_types.py` for the write.

pub mod expense_types;
