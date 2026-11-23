"""Alert condition names an image from before step 14 treats as retired: never
reused.
"""
from __future__ import annotations


ROLLBACK_RETIRED = {
    "warehouse_cutover.retired_conditions": frozenset({
        "attribution_coverage_website", "gold_cell_values", "gold_missing_cells",
        "gold_orphan_cells", "goods_shipped_without_sale", "headline_vs_line_items",
        "silver_missing_rows", "silver_orphan_rows", "silver_row_values",
    }),
    "duckdb_off.retired_conditions": frozenset({
        "pk_uniqueness_buyers", "pk_uniqueness_categories", "pk_uniqueness_products",
        "shadow_duckdb_only_rows", "shadow_missing_in_duckdb", "shadow_pruned_rows",
        "shadow_row_values",
    }),
    "pg_orders_write.retired_conditions": frozenset({
        "fk_orphan_order_products_order_id", "not_null_orders_ordered_at",
        "not_null_orders_source_id", "not_null_orders_status_id",
        "orders_without_line_items", "pk_uniqueness_order_products",
        "pk_uniqueness_orders", "status_group_vs_return_list",
        "value_domain_orders_source_id", "value_domain_orders_status_id",
    }),
}


ALL_RETIRED = frozenset().union(*ROLLBACK_RETIRED.values())
