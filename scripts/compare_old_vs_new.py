#!/usr/bin/env python3
"""Compare old (Bronze) vs new (Gold/Silver) query results.

This script runs the same queries using both the old implementation
(direct Bronze queries) and the new warehouse layers to verify
the numbers match exactly.
"""

import asyncio
import shutil
import tempfile
from datetime import date, timedelta
from pathlib import Path
from decimal import Decimal

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.duckdb_store import DuckDBStore, _date_in_kyiv
from core.models import OrderStatus


def format_diff(old, new, label=""):
    """Format comparison with diff indicator."""
    if isinstance(old, float) and isinstance(new, float):
        match = abs(old - new) < 0.01
    else:
        match = old == new

    status = "✓" if match else "✗ MISMATCH"
    return f"  {label}: OLD={old} vs NEW={new} {status}"


async def run_comparison():
    """Run old vs new query comparison."""

    src_db = Path("data/analytics.duckdb")
    if not src_db.exists():
        print("ERROR: data/analytics.duckdb not found")
        return False

    with tempfile.TemporaryDirectory() as tmpdir:
        test_db = Path(tmpdir) / "test_analytics.duckdb"
        print(f"Copying database to temp location...")
        shutil.copy(src_db, test_db)

        store = DuckDBStore(test_db)
        await store.connect()

        # First, refresh warehouse layers
        print("Refreshing warehouse layers...")
        result = await store.refresh_warehouse_layers(trigger="comparison_test")
        if result['status'] != 'success':
            print(f"ERROR: Refresh failed: {result.get('error')}")
            return False
        print(f"Refresh complete: {result['duration_ms']:.0f}ms\n")

        # Test parameters
        end_date = date.today()
        start_date = end_date - timedelta(days=30)
        sales_type = "retail"

        all_passed = True
        return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())

        # ═══════════════════════════════════════════════════════════════
        print("="*70)
        print("TEST 1: Summary Stats (no filters)")
        print("="*70)

        async with store.connection() as conn:
            # OLD: Direct Bronze query
            old_result = conn.execute(f"""
                SELECT
                    COUNT(DISTINCT CASE WHEN o.status_id NOT IN {return_statuses} THEN o.id END) as total_orders,
                    COALESCE(SUM(CASE WHEN o.status_id NOT IN {return_statuses} THEN o.grand_total END), 0) as total_revenue,
                    COUNT(DISTINCT CASE WHEN o.status_id IN {return_statuses} THEN o.id END) as total_returns
                FROM orders o
                WHERE {_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?
                  AND o.source_id IN (1, 2, 4)
                  AND (o.manager_id IS NULL
                       OR o.manager_id IN (SELECT id FROM managers WHERE is_retail = TRUE)
                       OR (NOT EXISTS (SELECT 1 FROM managers WHERE is_retail = TRUE)
                           AND o.manager_id IN (4, 8, 11, 16, 17, 19, 22)))
                  AND (o.manager_id IS NULL OR o.manager_id != 15)
            """, [start_date, end_date]).fetchone()

            old_orders = old_result[0]
            old_revenue = float(old_result[1])
            old_returns = old_result[2]

        # NEW: Gold layer query (via get_summary_stats)
        new_stats = await store.get_summary_stats(start_date, end_date, sales_type=sales_type)
        new_orders = new_stats['totalOrders']
        new_revenue = new_stats['totalRevenue']
        new_returns = new_stats['totalReturns']

        print(format_diff(old_orders, new_orders, "Orders"))
        print(format_diff(old_revenue, new_revenue, "Revenue"))
        print(format_diff(old_returns, new_returns, "Returns"))

        if old_orders != new_orders or abs(old_revenue - new_revenue) >= 0.01:
            all_passed = False

        # ═══════════════════════════════════════════════════════════════
        print("\n" + "="*70)
        print("TEST 2: Revenue by Day (first 5 days)")
        print("="*70)

        async with store.connection() as conn:
            # OLD: Direct Bronze query
            old_daily = conn.execute(f"""
                SELECT
                    {_date_in_kyiv('o.ordered_at')} as day,
                    SUM(o.grand_total) as revenue,
                    COUNT(DISTINCT o.id) as orders
                FROM orders o
                WHERE {_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?
                  AND o.status_id NOT IN {return_statuses}
                  AND o.source_id IN (1, 2, 4)
                  AND (o.manager_id IS NULL
                       OR o.manager_id IN (SELECT id FROM managers WHERE is_retail = TRUE)
                       OR (NOT EXISTS (SELECT 1 FROM managers WHERE is_retail = TRUE)
                           AND o.manager_id IN (4, 8, 11, 16, 17, 19, 22)))
                  AND (o.manager_id IS NULL OR o.manager_id != 15)
                GROUP BY {_date_in_kyiv('o.ordered_at')}
                ORDER BY day
                LIMIT 5
            """, [start_date, end_date]).fetchall()

        # NEW: Gold layer
        new_trend = await store.get_revenue_trend(start_date, end_date, sales_type=sales_type)

        for i, (old_day, old_rev, old_ord) in enumerate(old_daily):
            new_rev = new_trend['revenue'][i] if i < len(new_trend['revenue']) else 0
            new_ord = new_trend['orders'][i] if i < len(new_trend['orders']) else 0
            day_str = old_day.strftime("%Y-%m-%d")

            rev_match = abs(float(old_rev) - new_rev) < 0.01
            ord_match = old_ord == new_ord
            status = "✓" if (rev_match and ord_match) else "✗"

            print(f"  {day_str}: OLD(₴{float(old_rev):.2f}, {old_ord} ord) vs NEW(₴{new_rev:.2f}, {new_ord} ord) {status}")

            if not rev_match or not ord_match:
                all_passed = False

        # ═══════════════════════════════════════════════════════════════
        print("\n" + "="*70)
        print("TEST 3: Sales by Source")
        print("="*70)

        async with store.connection() as conn:
            # OLD: Direct Bronze query
            old_sources = conn.execute(f"""
                SELECT
                    o.source_id,
                    COUNT(DISTINCT o.id) as orders,
                    SUM(o.grand_total) as revenue
                FROM orders o
                WHERE {_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?
                  AND o.status_id NOT IN {return_statuses}
                  AND o.source_id IN (1, 2, 4)
                  AND (o.manager_id IS NULL
                       OR o.manager_id IN (SELECT id FROM managers WHERE is_retail = TRUE)
                       OR (NOT EXISTS (SELECT 1 FROM managers WHERE is_retail = TRUE)
                           AND o.manager_id IN (4, 8, 11, 16, 17, 19, 22)))
                  AND (o.manager_id IS NULL OR o.manager_id != 15)
                GROUP BY o.source_id
                ORDER BY revenue DESC
            """, [start_date, end_date]).fetchall()

        # NEW: Gold layer
        new_sources = await store.get_sales_by_source(start_date, end_date, sales_type=sales_type)

        source_names = {1: "Instagram", 2: "Telegram", 4: "Shopify"}
        old_by_name = {source_names[r[0]]: (r[1], float(r[2])) for r in old_sources}
        new_by_name = {new_sources['labels'][i]: (new_sources['orders'][i], new_sources['revenue'][i])
                       for i in range(len(new_sources['labels']))}

        for name in source_names.values():
            old_ord, old_rev = old_by_name.get(name, (0, 0))
            new_ord, new_rev = new_by_name.get(name, (0, 0))

            ord_match = old_ord == new_ord
            rev_match = abs(old_rev - new_rev) < 0.01
            status = "✓" if (ord_match and rev_match) else "✗"

            print(f"  {name}: OLD({old_ord} ord, ₴{old_rev:.2f}) vs NEW({new_ord} ord, ₴{new_rev:.2f}) {status}")

            if not ord_match or not rev_match:
                all_passed = False

        # ═══════════════════════════════════════════════════════════════
        print("\n" + "="*70)
        print("TEST 4: Summary with Category Filter")
        print("="*70)

        # Find a category with data
        async with store.connection() as conn:
            cat = conn.execute(f"""
                SELECT c.id, c.name
                FROM categories c
                JOIN products p ON p.category_id = c.id
                JOIN order_products op ON op.product_id = p.id
                JOIN orders o ON o.id = op.order_id
                WHERE {_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?
                  AND o.status_id NOT IN {return_statuses}
                  AND o.source_id IN (1, 2, 4)
                GROUP BY c.id, c.name
                HAVING COUNT(DISTINCT o.id) > 50
                ORDER BY COUNT(DISTINCT o.id) DESC
                LIMIT 1
            """, [start_date, end_date]).fetchone()

        if cat:
            cat_id, cat_name = cat
            print(f"  Testing with category: {cat_name} (id={cat_id})")

            # Get child categories
            async with store.connection() as conn:
                cat_ids = conn.execute("""
                    WITH RECURSIVE cat_tree AS (
                        SELECT id FROM categories WHERE id = ?
                        UNION ALL
                        SELECT c.id FROM categories c JOIN cat_tree ct ON c.parent_id = ct.id
                    )
                    SELECT id FROM cat_tree
                """, [cat_id]).fetchall()
                cat_ids = [r[0] for r in cat_ids]
                cat_ids_str = ','.join(str(c) for c in cat_ids)

                # OLD: Direct Bronze query with category
                old_cat = conn.execute(f"""
                    SELECT
                        COUNT(DISTINCT o.id) as orders,
                        COALESCE(SUM(op.price_sold * op.quantity), 0) as revenue
                    FROM orders o
                    JOIN order_products op ON o.id = op.order_id
                    LEFT JOIN products p ON op.product_id = p.id
                    WHERE {_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?
                      AND o.status_id NOT IN {return_statuses}
                      AND o.source_id IN (1, 2, 4)
                      AND (o.manager_id IS NULL
                           OR o.manager_id IN (SELECT id FROM managers WHERE is_retail = TRUE)
                           OR (NOT EXISTS (SELECT 1 FROM managers WHERE is_retail = TRUE)
                               AND o.manager_id IN (4, 8, 11, 16, 17, 19, 22)))
                      AND (o.manager_id IS NULL OR o.manager_id != 15)
                      AND p.category_id IN ({cat_ids_str})
                """, [start_date, end_date]).fetchone()

            old_cat_orders = old_cat[0]
            old_cat_revenue = float(old_cat[1])

            # NEW: Silver layer (via get_summary_stats with category)
            new_cat_stats = await store.get_summary_stats(
                start_date, end_date, category_id=cat_id, sales_type=sales_type
            )
            new_cat_orders = new_cat_stats['totalOrders']
            new_cat_revenue = new_cat_stats['totalRevenue']

            print(format_diff(old_cat_orders, new_cat_orders, "Orders"))
            print(format_diff(old_cat_revenue, new_cat_revenue, "Revenue"))

            if old_cat_orders != new_cat_orders or abs(old_cat_revenue - new_cat_revenue) >= 0.01:
                all_passed = False
        else:
            print("  No category with enough data found, skipping")

        # ═══════════════════════════════════════════════════════════════
        print("\n" + "="*70)
        print("TEST 5: Top Products")
        print("="*70)

        async with store.connection() as conn:
            # OLD: Direct Bronze query
            old_products = conn.execute(f"""
                SELECT
                    op.name,
                    SUM(op.quantity) as qty
                FROM orders o
                JOIN order_products op ON o.id = op.order_id
                WHERE {_date_in_kyiv('o.ordered_at')} BETWEEN ? AND ?
                  AND o.status_id NOT IN {return_statuses}
                  AND o.source_id IN (1, 2, 4)
                  AND (o.manager_id IS NULL
                       OR o.manager_id IN (SELECT id FROM managers WHERE is_retail = TRUE)
                       OR (NOT EXISTS (SELECT 1 FROM managers WHERE is_retail = TRUE)
                           AND o.manager_id IN (4, 8, 11, 16, 17, 19, 22)))
                  AND (o.manager_id IS NULL OR o.manager_id != 15)
                GROUP BY op.name
                ORDER BY qty DESC
                LIMIT 5
            """, [start_date, end_date]).fetchall()

        # NEW: Gold layer
        new_products = await store.get_top_products(start_date, end_date, sales_type=sales_type, limit=5)

        for i, (old_name, old_qty) in enumerate(old_products):
            new_name = new_products['labels'][i] if i < len(new_products['labels']) else "N/A"
            new_qty = new_products['data'][i] if i < len(new_products['data']) else 0

            name_match = old_name == new_name
            qty_match = old_qty == new_qty
            status = "✓" if (name_match and qty_match) else "✗"

            print(f"  #{i+1}: OLD({old_name[:30]}, {old_qty}) vs NEW({new_name[:30]}, {new_qty}) {status}")

            if not name_match or not qty_match:
                all_passed = False

        # ═══════════════════════════════════════════════════════════════
        print("\n" + "="*70)
        if all_passed:
            print("ALL COMPARISONS PASSED ✓")
        else:
            print("SOME COMPARISONS FAILED ✗")
        print("="*70)

        return all_passed


if __name__ == "__main__":
    success = asyncio.run(run_comparison())
    sys.exit(0 if success else 1)
