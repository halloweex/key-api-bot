//! The warehouse's shapes, in one home: Silver's column order and Gold's
//! cells and measures.
//!
//! `GOLD_MEASURES` (`core/warehouse_sql.py`) is the one text Postgres and
//! ClickHouse both render, and it is engine-neutral by construction —
//! `COUNT(DISTINCT CASE WHEN … THEN … END)` and `COALESCE(SUM(CASE …), 0)`,
//! ordinary aggregate SQL that ClickHouse 24.8 computes with Postgres'
//! meaning — so there is no third dialect of the measures here either: the
//! same expressions, character for character. `contract/ch_sync.json` holds
//! them to Python's, and the day a measure moves in one language and not the
//! other the contract test fails.

/// `core.pg_silver.SILVER_COLUMNS`: the projection's own write order.
pub const SILVER_COLUMNS: [&str; 15] = [
    "id",
    "source_id",
    "status_id",
    "grand_total",
    "ordered_at",
    "buyer_id",
    "manager_id",
    "order_date",
    "is_return",
    "sales_type",
    "is_active_source",
    "source_name",
    "is_new_customer",
    "buyer_first_order_date",
    "promocode",
];

/// `core.pg_gold.GOLD_DIMENSIONS`: a cell is a day, a sales type and a
/// source, the source NULL on the roll-up.
pub const GOLD_DIMENSIONS: [&str; 3] = ["date", "sales_type", "source_id"];

/// `core.pg_gold.GOLD_MEASURE_COLUMNS`.
pub const GOLD_MEASURE_COLUMNS: [&str; 8] = [
    "revenue",
    "orders_count",
    "unique_customers",
    "new_customers",
    "returning_customers",
    "returns_count",
    "returns_revenue",
    "avg_order_value",
];

/// `core.pg_gold.GOLD_COLUMNS`: the dimensions, then the measures.
pub fn gold_columns() -> Vec<&'static str> {
    GOLD_DIMENSIONS
        .iter()
        .chain(GOLD_MEASURE_COLUMNS.iter())
        .copied()
        .collect()
}

/// The rows every revenue measure reads, spelled once.
pub const EARNING_ROWS: &str = "NOT is_return AND is_active_source";
/// The rows every return measure reads.
pub const RETURNED_ROWS: &str = "is_return AND is_active_source";

/// `core.warehouse_sql.GOLD_MEASURES`, in its order: each measure's name
/// and the expression every engine renders.
pub fn gold_measures() -> Vec<(&'static str, String)> {
    let revenue = format!("COALESCE(SUM(CASE WHEN {EARNING_ROWS} THEN grand_total END), 0)");
    let orders = format!("COUNT(DISTINCT CASE WHEN {EARNING_ROWS} THEN id END)");
    vec![
        ("revenue", revenue.clone()),
        ("orders_count", orders.clone()),
        (
            "unique_customers",
            format!(
                "COUNT(DISTINCT CASE WHEN {EARNING_ROWS} AND buyer_id IS NOT NULL THEN buyer_id END)"
            ),
        ),
        (
            "new_customers",
            format!(
                "COUNT(DISTINCT CASE WHEN {EARNING_ROWS} AND is_new_customer THEN buyer_id END)"
            ),
        ),
        (
            "returning_customers",
            format!(
                "COUNT(DISTINCT CASE WHEN {EARNING_ROWS} AND NOT is_new_customer AND buyer_id IS NOT NULL THEN buyer_id END)"
            ),
        ),
        (
            "returns_count",
            format!("COUNT(DISTINCT CASE WHEN {RETURNED_ROWS} THEN id END)"),
        ),
        (
            "returns_revenue",
            format!("COALESCE(SUM(CASE WHEN {RETURNED_ROWS} THEN grand_total END), 0)"),
        ),
        // Written out rather than `revenue / orders_count`: the guard and the
        // division must read the same aggregate, and an alias is not
        // available to a sibling select item in either engine.
        (
            "avg_order_value",
            format!(
                "CASE\n        WHEN {orders} > 0\n        THEN {revenue}\n             / {orders}\n        ELSE 0\n    END"
            ),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_measure_column_has_its_measure_in_order() {
        let names: Vec<&str> = gold_measures().iter().map(|(name, _)| *name).collect();
        assert_eq!(names, GOLD_MEASURE_COLUMNS);
        assert_eq!(gold_columns().len(), 11);
    }
}
