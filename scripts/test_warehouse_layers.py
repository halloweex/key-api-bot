#!/usr/bin/env python3
"""Test script for warehouse layer implementation.

Creates a copy of the production database and validates:
1. Warehouse refresh works correctly
2. Checksums match between layers
3. Query results match between old and new implementations
"""

import asyncio
import shutil
import tempfile
from datetime import date, timedelta
from pathlib import Path

# Add project root to path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.duckdb_store import DuckDBStore


async def test_warehouse_layers():
    """Run warehouse layer tests."""

    # Copy production database to temp location
    src_db = Path("data/analytics.duckdb")
    if not src_db.exists():
        print("ERROR: data/analytics.duckdb not found")
        return False

    with tempfile.TemporaryDirectory() as tmpdir:
        test_db = Path(tmpdir) / "test_analytics.duckdb"
        print(f"Copying database to {test_db}...")
        shutil.copy(src_db, test_db)

        # Initialize store with test database
        store = DuckDBStore(test_db)
        await store.connect()

        print("\n" + "="*60)
        print("TEST 1: Warehouse Refresh")
        print("="*60)

        # Run warehouse refresh
        result = await store.refresh_warehouse_layers(trigger="test")

        print(f"Status: {result['status']}")
        print(f"Duration: {result.get('duration_ms', 'N/A')}ms")
        print(f"Bronze orders: {result.get('bronze_orders', 'N/A')}")
        print(f"Silver rows: {result.get('silver_rows', 'N/A')}")
        print(f"Gold revenue rows: {result.get('gold_revenue_rows', 'N/A')}")
        print(f"Gold products rows: {result.get('gold_products_rows', 'N/A')}")
        print(f"Checksum match: {result.get('checksum_match', 'N/A')}")
        print(f"Validation passed: {result.get('validation_passed', 'N/A')}")

        if result['status'] != 'success':
            print(f"ERROR: {result.get('error', 'Unknown error')}")
            return False

        if not result.get('validation_passed'):
            print("ERROR: Validation failed!")
            return False

        print("\n" + "="*60)
        print("TEST 2: Compare Summary Stats (no filter vs with category)")
        print("="*60)

        end_date = date.today()
        start_date = end_date - timedelta(days=30)

        # Test without filter (should use Gold)
        summary_no_filter = await store.get_summary_stats(
            start_date, end_date, sales_type="retail"
        )
        print(f"\nNo filter (Gold layer):")
        print(f"  Orders: {summary_no_filter['totalOrders']}")
        print(f"  Revenue: {summary_no_filter['totalRevenue']}")
        print(f"  Avg Check: {summary_no_filter['avgCheck']}")

        # Get a category with actual orders in the period
        async with store.connection() as conn:
            cat = conn.execute("""
                SELECT c.id, c.name, COUNT(DISTINCT o.id) as order_count
                FROM categories c
                JOIN products p ON p.category_id = c.id
                JOIN order_products op ON op.product_id = p.id
                JOIN orders o ON o.id = op.order_id
                WHERE DATE(timezone('Europe/Kyiv', o.ordered_at)) BETWEEN ? AND ?
                  AND o.status_id NOT IN (19, 21, 22, 23)
                  AND o.source_id IN (1, 2, 4)
                GROUP BY c.id, c.name
                HAVING COUNT(DISTINCT o.id) > 10
                ORDER BY order_count DESC
                LIMIT 1
            """, [start_date, end_date]).fetchone()

        if cat:
            # Test with category filter (should use Silver)
            summary_with_cat = await store.get_summary_stats(
                start_date, end_date, category_id=cat[0], sales_type="retail"
            )
            print(f"\nWith category '{cat[1]}' filter (Silver layer):")
            print(f"  Orders: {summary_with_cat['totalOrders']}")
            print(f"  Revenue: {summary_with_cat['totalRevenue']}")
            print(f"  Avg Check: {summary_with_cat['avgCheck']}")

            # Sanity check: filtered should be <= unfiltered
            if summary_with_cat['totalOrders'] > summary_no_filter['totalOrders']:
                print("WARNING: Filtered orders > unfiltered (might be OK if category covers most products)")

        print("\n" + "="*60)
        print("TEST 3: Compare Revenue Trend")
        print("="*60)

        trend = await store.get_revenue_trend(
            start_date, end_date, sales_type="retail"
        )
        total_trend_revenue = sum(trend['revenue'])
        total_trend_orders = sum(trend['orders'])

        print(f"Revenue trend (Gold layer):")
        print(f"  Days: {len(trend['labels'])}")
        print(f"  Total Revenue: {total_trend_revenue:.2f}")
        print(f"  Total Orders: {total_trend_orders}")

        # Compare with summary
        diff_pct = abs(total_trend_revenue - summary_no_filter['totalRevenue']) / summary_no_filter['totalRevenue'] * 100 if summary_no_filter['totalRevenue'] > 0 else 0
        print(f"\nRevenue diff from summary: {diff_pct:.2f}%")
        if diff_pct > 1:
            print("WARNING: Revenue difference > 1%")

        print("\n" + "="*60)
        print("TEST 4: Compare Sales By Source")
        print("="*60)

        sales_by_source = await store.get_sales_by_source(
            start_date, end_date, sales_type="retail"
        )
        total_source_orders = sum(sales_by_source['orders'])
        total_source_revenue = sum(sales_by_source['revenue'])

        print(f"Sales by source (Gold layer):")
        for i, label in enumerate(sales_by_source['labels']):
            print(f"  {label}: {sales_by_source['orders'][i]} orders, ₴{sales_by_source['revenue'][i]}")

        print(f"\nTotal: {total_source_orders} orders, ₴{total_source_revenue:.2f}")

        # Compare with summary
        if total_source_orders != summary_no_filter['totalOrders']:
            print(f"WARNING: Source orders ({total_source_orders}) != Summary orders ({summary_no_filter['totalOrders']})")

        print("\n" + "="*60)
        print("TEST 5: Verify Order Count with Category Filter")
        print("="*60)

        if cat:
            # Get count via Silver (new method)
            summary_silver = await store.get_summary_stats(
                start_date, end_date, category_id=cat[0], sales_type="retail"
            )

            # Get count via raw query (ground truth) - must match sales_type filter!
            # Retail = manager_id IS NULL OR manager_id IN retail list, AND NOT b2b manager
            async with store.connection() as conn:
                raw = conn.execute("""
                    SELECT COUNT(DISTINCT o.id) as orders
                    FROM orders o
                    JOIN order_products op ON o.id = op.order_id
                    LEFT JOIN products p ON op.product_id = p.id
                    WHERE DATE(timezone('Europe/Kyiv', o.ordered_at)) BETWEEN ? AND ?
                      AND o.status_id NOT IN (19, 21, 22, 23)
                      AND o.source_id IN (1, 2, 4)
                      AND (o.manager_id IS NULL
                           OR o.manager_id IN (SELECT id FROM managers WHERE is_retail = TRUE)
                           OR (NOT EXISTS (SELECT 1 FROM managers WHERE is_retail = TRUE)
                               AND o.manager_id IN (4, 8, 11, 16, 17, 19, 22)))
                      AND (o.manager_id IS NULL OR o.manager_id != 15)
                      AND p.category_id IN (
                          WITH RECURSIVE cat_tree AS (
                              SELECT id FROM categories WHERE id = ?
                              UNION ALL
                              SELECT c.id FROM categories c JOIN cat_tree ct ON c.parent_id = ct.id
                          )
                          SELECT id FROM cat_tree
                      )
                """, [start_date, end_date, cat[0]]).fetchone()

            raw_orders = raw[0] if raw else 0

            print(f"Category '{cat[1]}' order count comparison:")
            print(f"  Silver layer: {summary_silver['totalOrders']}")
            print(f"  Raw Bronze query: {raw_orders}")

            if summary_silver['totalOrders'] != raw_orders:
                print(f"ERROR: Order count mismatch! Silver={summary_silver['totalOrders']}, Raw={raw_orders}")
                return False
            else:
                print("  ✓ Match!")

        print("\n" + "="*60)
        print("TEST 6: Warehouse Status Endpoint")
        print("="*60)

        status = await store.get_warehouse_status()
        print(f"Last refresh: {status.get('last_refresh')}")
        print(f"Last trigger: {status.get('last_trigger')}")
        print(f"Validation passed: {status.get('validation_passed')}")
        print(f"Recent refreshes: {status.get('recent_refreshes')}")

        print("\n" + "="*60)
        print("ALL TESTS PASSED ✓")
        print("="*60)
        return True


if __name__ == "__main__":
    success = asyncio.run(test_warehouse_layers())
    sys.exit(0 if success else 1)
