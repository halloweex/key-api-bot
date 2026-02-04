#!/usr/bin/env python3
"""
Check orders for a date range - compare DuckDB vs KeyCRM API.

Usage:
    python scripts/check_range.py 2026-01-16 2026-01-31
"""
import asyncio
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.keycrm import KeyCRMClient
from core.models import OrderStatus

KYIV_TZ = ZoneInfo("Europe/Kyiv")
RETURN_STATUSES = set(int(s) for s in OrderStatus.return_statuses())
SOURCE_NAMES = {1: "Instagram", 2: "Telegram", 4: "Shopify"}


async def get_duckdb_data(start_date: date, end_date: date):
    """Get aggregated data from DuckDB for date range."""
    import duckdb
    db_path = Path(__file__).parent.parent / "data" / "analytics.duckdb"
    conn = duckdb.connect(str(db_path), read_only=True)

    # Get by-source breakdown
    results = conn.execute("""
        SELECT
            source_id,
            COUNT(*) as order_count,
            SUM(grand_total) as revenue,
            COUNT(*) FILTER (WHERE status_id IN (19, 22, 21, 23)) as return_count
        FROM orders
        WHERE DATE(ordered_at) BETWEEN ? AND ?
          AND source_id IN (1, 2, 4)
        GROUP BY source_id
        ORDER BY source_id
    """, [start_date, end_date]).fetchall()

    by_source = {}
    total_orders = 0
    total_revenue = 0
    total_returns = 0

    for source_id, count, revenue, returns in results:
        name = SOURCE_NAMES.get(source_id, f"Source {source_id}")
        non_return = count - returns
        non_return_rev = revenue  # We'll calculate this properly

        # Get non-return revenue separately
        rev_result = conn.execute("""
            SELECT SUM(grand_total)
            FROM orders
            WHERE DATE(ordered_at) BETWEEN ? AND ?
              AND source_id = ?
              AND status_id NOT IN (19, 22, 21, 23)
        """, [start_date, end_date, source_id]).fetchone()
        non_return_rev = rev_result[0] or 0

        by_source[name] = {
            "orders": non_return,
            "revenue": float(non_return_rev),
            "returns": returns
        }
        total_orders += non_return
        total_revenue += non_return_rev
        total_returns += returns

    conn.close()

    return {
        "total_orders": total_orders,
        "total_revenue": float(total_revenue),
        "total_returns": total_returns,
        "by_source": by_source
    }


async def get_keycrm_data(start_date: date, end_date: date):
    """Get aggregated data from KeyCRM API for date range."""
    client = KeyCRMClient()
    await client.connect()

    # Fetch all orders in the range (with buffer for timezone)
    buffer_start = start_date - timedelta(days=1)
    buffer_end = end_date + timedelta(days=1)

    all_orders = []
    params = {
        "include": "products.offer",
        "filter[created_between]": f"{buffer_start}, {buffer_end}",
    }

    print(f"Fetching orders from KeyCRM API...")
    async for batch in client.paginate("order", params=params, page_size=50):
        all_orders.extend(batch)
    print(f"  Fetched {len(all_orders)} orders from created_between")

    # Also fetch by ordered_at if available
    try:
        params_ordered = {
            "include": "products.offer",
            "filter[ordered_between]": f"{buffer_start}, {buffer_end}",
        }
        async for batch in client.paginate("order", params=params_ordered, page_size=50):
            for order in batch:
                if order["id"] not in [o["id"] for o in all_orders]:
                    all_orders.append(order)
        print(f"  After ordered_between: {len(all_orders)} orders")
    except Exception as e:
        print(f"  ordered_between not available: {e}")

    # Filter to orders with ordered_at in date range (Kyiv timezone)
    target_orders = []
    for order in all_orders:
        ordered_at_str = order.get("ordered_at")
        if ordered_at_str:
            try:
                ordered_at = datetime.fromisoformat(ordered_at_str.replace("Z", "+00:00"))
                ordered_at_kyiv = ordered_at.astimezone(KYIV_TZ)
                if start_date <= ordered_at_kyiv.date() <= end_date:
                    # Only include relevant sources
                    if order.get("source_id") in (1, 2, 4):
                        target_orders.append(order)
            except (ValueError, TypeError):
                pass

    print(f"  Filtered to {len(target_orders)} orders in date range")

    # Calculate stats
    by_source = {}
    total_orders = 0
    total_revenue = 0
    total_returns = 0

    for order in target_orders:
        source_id = order.get("source_id")
        name = SOURCE_NAMES.get(source_id, f"Source {source_id}")
        status_id = order.get("status_id")
        is_return = status_id in RETURN_STATUSES

        if name not in by_source:
            by_source[name] = {"orders": 0, "revenue": 0.0, "returns": 0}

        if is_return:
            by_source[name]["returns"] += 1
            total_returns += 1
        else:
            by_source[name]["orders"] += 1
            by_source[name]["revenue"] += float(order.get("grand_total", 0))
            total_orders += 1
            total_revenue += float(order.get("grand_total", 0))

    await client.close()

    return {
        "total_orders": total_orders,
        "total_revenue": total_revenue,
        "total_returns": total_returns,
        "by_source": by_source
    }


async def main(start_str: str, end_str: str):
    start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_str, "%Y-%m-%d").date()

    print(f"\n{'='*70}")
    print(f"Comparing data: {start_date} to {end_date}")
    print(f"{'='*70}")

    # Get data from both sources
    print("\n[1/2] Fetching from DuckDB...")
    duckdb_data = await get_duckdb_data(start_date, end_date)

    print("\n[2/2] Fetching from KeyCRM API...")
    keycrm_data = await get_keycrm_data(start_date, end_date)

    # Print comparison
    print(f"\n{'='*70}")
    print(f"{'COMPARISON RESULTS':^70}")
    print(f"{'='*70}")

    print(f"\n{'Metric':<20} {'DuckDB':>18} {'KeyCRM API':>18} {'Diff':>12}")
    print("-" * 70)

    # Totals
    order_diff = keycrm_data["total_orders"] - duckdb_data["total_orders"]
    rev_diff = keycrm_data["total_revenue"] - duckdb_data["total_revenue"]
    ret_diff = keycrm_data["total_returns"] - duckdb_data["total_returns"]

    print(f"{'Orders':<20} {duckdb_data['total_orders']:>18,} {keycrm_data['total_orders']:>18,} {order_diff:>+12,}")
    print(f"{'Revenue':<20} {'₴{:,.2f}'.format(duckdb_data['total_revenue']):>18} {'₴{:,.2f}'.format(keycrm_data['total_revenue']):>18} {'₴{:+,.2f}'.format(rev_diff):>12}")
    print(f"{'Returns':<20} {duckdb_data['total_returns']:>18,} {keycrm_data['total_returns']:>18,} {ret_diff:>+12,}")

    # By source
    print(f"\n{'BY SOURCE':^70}")
    print("-" * 70)

    all_sources = set(duckdb_data["by_source"].keys()) | set(keycrm_data["by_source"].keys())

    for source in sorted(all_sources):
        db = duckdb_data["by_source"].get(source, {"orders": 0, "revenue": 0, "returns": 0})
        kc = keycrm_data["by_source"].get(source, {"orders": 0, "revenue": 0, "returns": 0})

        print(f"\n{source}:")
        order_diff = kc["orders"] - db["orders"]
        rev_diff = kc["revenue"] - db["revenue"]
        ret_diff = kc["returns"] - db["returns"]

        status_orders = "✓" if order_diff == 0 else "✗"
        status_rev = "✓" if abs(rev_diff) < 100 else "✗"
        status_ret = "✓" if ret_diff == 0 else "✗"

        print(f"  {'Orders':<18} {db['orders']:>16,} {kc['orders']:>16,} {order_diff:>+10,} {status_orders}")
        print(f"  {'Revenue':<18} {'₴{:,.2f}'.format(db['revenue']):>16} {'₴{:,.2f}'.format(kc['revenue']):>16} {'₴{:+,.2f}'.format(rev_diff):>10} {status_rev}")
        print(f"  {'Returns':<18} {db['returns']:>16,} {kc['returns']:>16,} {ret_diff:>+10,} {status_ret}")

    # Summary
    print(f"\n{'='*70}")
    if order_diff == 0 and abs(rev_diff) < 100 and ret_diff == 0:
        print("✓ Data matches within tolerance!")
    else:
        print("✗ Discrepancies found!")
        if order_diff != 0:
            print(f"  - Order count differs by {order_diff:+,}")
        if abs(rev_diff) >= 100:
            print(f"  - Revenue differs by ₴{rev_diff:+,.2f}")
        if ret_diff != 0:
            print(f"  - Return count differs by {ret_diff:+,}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python scripts/check_range.py START_DATE END_DATE")
        print("Example: python scripts/check_range.py 2026-01-16 2026-01-31")
        sys.exit(1)

    asyncio.run(main(sys.argv[1], sys.argv[2]))
