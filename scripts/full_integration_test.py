#!/usr/bin/env python3
"""Full integration test for warehouse layers.

Tests all API endpoints and edge cases.
"""

import asyncio
import shutil
import tempfile
from datetime import date, timedelta
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.duckdb_store import DuckDBStore


async def run_full_test():
    """Run comprehensive integration tests."""

    src_db = Path("data/analytics.duckdb")
    if not src_db.exists():
        print("ERROR: data/analytics.duckdb not found")
        return False

    with tempfile.TemporaryDirectory() as tmpdir:
        test_db = Path(tmpdir) / "test_analytics.duckdb"
        shutil.copy(src_db, test_db)

        store = DuckDBStore(test_db)
        await store.connect()

        # Refresh warehouse
        print("Refreshing warehouse layers...")
        result = await store.refresh_warehouse_layers(trigger="full_test")
        assert result['status'] == 'success', f"Refresh failed: {result}"
        assert result['validation_passed'], "Validation failed"
        print(f"✓ Warehouse refresh OK ({result['duration_ms']:.0f}ms)\n")

        end_date = date.today()
        start_date = end_date - timedelta(days=30)
        all_passed = True

        # ═══════════════════════════════════════════════════════════════
        print("TEST 1: get_summary_stats (multiple combinations)")
        print("-" * 50)

        # No filters
        r = await store.get_summary_stats(start_date, end_date, sales_type="retail")
        assert r['totalOrders'] > 0, "No orders found"
        assert r['totalRevenue'] > 0, "No revenue found"
        print(f"  ✓ No filters: {r['totalOrders']} orders, ₴{r['totalRevenue']:.2f}")

        # With source_id
        r = await store.get_summary_stats(start_date, end_date, source_id=1, sales_type="retail")
        assert r['totalOrders'] >= 0
        print(f"  ✓ source_id=1 (Instagram): {r['totalOrders']} orders")

        # With source_id=4 (Shopify)
        r = await store.get_summary_stats(start_date, end_date, source_id=4, sales_type="retail")
        print(f"  ✓ source_id=4 (Shopify): {r['totalOrders']} orders")

        # sales_type=all
        r = await store.get_summary_stats(start_date, end_date, sales_type="all")
        print(f"  ✓ sales_type=all: {r['totalOrders']} orders")

        # sales_type=b2b
        r = await store.get_summary_stats(start_date, end_date, sales_type="b2b")
        print(f"  ✓ sales_type=b2b: {r['totalOrders']} orders")

        # ═══════════════════════════════════════════════════════════════
        print("\nTEST 2: get_revenue_trend (multiple combinations)")
        print("-" * 50)

        # No filters
        r = await store.get_revenue_trend(start_date, end_date, sales_type="retail")
        assert len(r['labels']) == 31, f"Expected 31 days, got {len(r['labels'])}"
        assert len(r['revenue']) == 31
        assert len(r['orders']) == 31
        print(f"  ✓ No filters: {len(r['labels'])} days, total ₴{sum(r['revenue']):.2f}")

        # With comparison
        r = await store.get_revenue_trend(start_date, end_date, sales_type="retail", include_comparison=True)
        assert 'comparison' in r, "Missing comparison data"
        print(f"  ✓ With comparison: growth={r['comparison']['totals']['growth_percent']:.1f}%")

        # With source_id
        r = await store.get_revenue_trend(start_date, end_date, source_id=1, sales_type="retail")
        print(f"  ✓ source_id=1: total ₴{sum(r['revenue']):.2f}")

        # ═══════════════════════════════════════════════════════════════
        print("\nTEST 3: get_sales_by_source")
        print("-" * 50)

        r = await store.get_sales_by_source(start_date, end_date, sales_type="retail")
        assert len(r['labels']) > 0, "No sources found"
        total_orders = sum(r['orders'])
        print(f"  ✓ {len(r['labels'])} sources, {total_orders} total orders")
        for i, label in enumerate(r['labels']):
            print(f"    - {label}: {r['orders'][i]} orders, ₴{r['revenue'][i]:.2f}")

        # ═══════════════════════════════════════════════════════════════
        print("\nTEST 4: get_top_products")
        print("-" * 50)

        r = await store.get_top_products(start_date, end_date, sales_type="retail", limit=5)
        assert len(r['labels']) <= 5, f"Expected max 5 products, got {len(r['labels'])}"
        print(f"  ✓ Top {len(r['labels'])} products:")
        for i, label in enumerate(r['labels'][:3]):
            print(f"    - {label[:40]}: {r['data'][i]} units")

        # ═══════════════════════════════════════════════════════════════
        print("\nTEST 5: get_product_performance")
        print("-" * 50)

        r = await store.get_product_performance(start_date, end_date, sales_type="retail")
        assert 'topByRevenue' in r
        assert 'categoryBreakdown' in r
        print(f"  ✓ Top by revenue: {len(r['topByRevenue']['labels'])} products")
        print(f"  ✓ Categories: {len(r['categoryBreakdown']['labels'])} categories")

        # ═══════════════════════════════════════════════════════════════
        print("\nTEST 6: get_brand_analytics")
        print("-" * 50)

        r = await store.get_brand_analytics(start_date, end_date, sales_type="retail")
        assert 'topByRevenue' in r
        assert 'topByQuantity' in r
        print(f"  ✓ {r['metrics']['totalBrands']} brands")
        print(f"  ✓ Top brand: {r['metrics']['topBrand']} ({r['metrics']['topBrandShare']:.1f}% share)")

        # ═══════════════════════════════════════════════════════════════
        print("\nTEST 7: get_customer_insights")
        print("-" * 50)

        r = await store.get_customer_insights(start_date, end_date, sales_type="retail")
        assert 'metrics' in r
        print(f"  ✓ New: {r['metrics']['newCustomers']}, Returning: {r['metrics']['returningCustomers']}")
        print(f"  ✓ CLV: ₴{r['metrics']['customerLifetimeValue']:.2f}")
        print(f"  ✓ True repeat rate: {r['metrics']['trueRepeatRate']:.1f}%")

        # ═══════════════════════════════════════════════════════════════
        print("\nTEST 8: Category filter (uses Silver layer)")
        print("-" * 50)

        # Find a category with data
        async with store.connection() as conn:
            cat = conn.execute("""
                SELECT c.id, c.name FROM categories c
                JOIN products p ON p.category_id = c.id
                JOIN order_products op ON op.product_id = p.id
                GROUP BY c.id, c.name
                HAVING COUNT(*) > 100
                LIMIT 1
            """).fetchone()

        if cat:
            cat_id, cat_name = cat
            print(f"  Testing with category: {cat_name}")

            r = await store.get_summary_stats(start_date, end_date, category_id=cat_id, sales_type="retail")
            print(f"  ✓ Summary: {r['totalOrders']} orders, ₴{r['totalRevenue']:.2f}")

            r = await store.get_revenue_trend(start_date, end_date, category_id=cat_id, sales_type="retail")
            print(f"  ✓ Revenue trend: {len(r['labels'])} days")

            r = await store.get_sales_by_source(start_date, end_date, category_id=cat_id, sales_type="retail")
            print(f"  ✓ Sales by source: {len(r['labels'])} sources")
        else:
            print("  ⚠ No category with enough data found")

        # ═══════════════════════════════════════════════════════════════
        print("\nTEST 9: Brand filter (uses Silver layer)")
        print("-" * 50)

        # Find a brand with data
        async with store.connection() as conn:
            brand_row = conn.execute("""
                SELECT p.brand FROM products p
                JOIN order_products op ON op.product_id = p.id
                WHERE p.brand IS NOT NULL AND p.brand != ''
                GROUP BY p.brand
                HAVING COUNT(*) > 50
                LIMIT 1
            """).fetchone()

        if brand_row:
            brand_name = brand_row[0]
            print(f"  Testing with brand: {brand_name}")

            r = await store.get_summary_stats(start_date, end_date, brand=brand_name, sales_type="retail")
            print(f"  ✓ Summary: {r['totalOrders']} orders, ₴{r['totalRevenue']:.2f}")

            r = await store.get_revenue_trend(start_date, end_date, brand=brand_name, sales_type="retail")
            print(f"  ✓ Revenue trend: {len(r['labels'])} days")
        else:
            print("  ⚠ No brand with enough data found")

        # ═══════════════════════════════════════════════════════════════
        print("\nTEST 10: get_warehouse_status")
        print("-" * 50)

        r = await store.get_warehouse_status()
        assert r['last_refresh'] is not None
        assert r['validation_passed'] == True
        print(f"  ✓ Last refresh: {r['last_trigger']}")
        print(f"  ✓ Bronze: {r['bronze_orders']}, Silver: {r['silver_rows']}")
        print(f"  ✓ Gold revenue: {r['gold_revenue_rows']}, Gold products: {r['gold_products_rows']}")

        # ═══════════════════════════════════════════════════════════════
        print("\nTEST 11: Edge cases")
        print("-" * 50)

        # Empty date range (future)
        future_start = date.today() + timedelta(days=100)
        future_end = future_start + timedelta(days=7)
        r = await store.get_summary_stats(future_start, future_end, sales_type="retail")
        assert r['totalOrders'] == 0, "Expected 0 orders for future dates"
        print(f"  ✓ Future dates: {r['totalOrders']} orders (correct)")

        # Single day
        r = await store.get_summary_stats(end_date, end_date, sales_type="retail")
        print(f"  ✓ Single day: {r['totalOrders']} orders")

        # Invalid source_id (should return 0)
        r = await store.get_summary_stats(start_date, end_date, source_id=999, sales_type="retail")
        assert r['totalOrders'] == 0, "Expected 0 for invalid source"
        print(f"  ✓ Invalid source_id: {r['totalOrders']} orders (correct)")

        # ═══════════════════════════════════════════════════════════════
        print("\n" + "="*50)
        print("ALL INTEGRATION TESTS PASSED ✓")
        print("="*50)

        return True


if __name__ == "__main__":
    success = asyncio.run(run_full_test())
    sys.exit(0 if success else 1)
