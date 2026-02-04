#!/usr/bin/env python3
"""
Check orders for a date range - compare Dashboard API vs KeyCRM API.

Usage:
    python scripts/check_range_api.py 2026-01-16 2026-01-31
"""
import asyncio
import sys
import httpx
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
API_BASE = "http://localhost:8080/api"


async def get_dashboard_data(start_date: date, end_date: date):
    """Get aggregated data from Dashboard API for date range."""
    async with httpx.AsyncClient() as client:
        # Get summary
        params = {
            "start_date": str(start_date),
            "end_date": str(end_date),
            "sales_type": "all"
        }
        summary_resp = await client.get(f"{API_BASE}/summary", params=params)
        summary = summary_resp.json()

        # Get by-source breakdown
        source_resp = await client.get(f"{API_BASE}/sales/by-source", params=params)
        source_data = source_resp.json()

        by_source = {}
        for i, label in enumerate(source_data.get("labels", [])):
            by_source[label] = {
                "orders": source_data["orders"][i],
                "revenue": float(source_data["revenue"][i]),
            }

        return {
            "total_orders": summary["totalOrders"],
            "total_revenue": float(summary["totalRevenue"]),
            "total_returns": summary["totalReturns"],
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
    print("\n[1/2] Fetching from Dashboard API (DuckDB)...")
    dashboard_data = await get_dashboard_data(start_date, end_date)

    print("\n[2/2] Fetching from KeyCRM API...")
    keycrm_data = await get_keycrm_data(start_date, end_date)

    # Print comparison
    print(f"\n{'='*70}")
    print(f"{'COMPARISON RESULTS':^70}")
    print(f"{'='*70}")

    print(f"\n{'Metric':<20} {'Dashboard':>18} {'KeyCRM API':>18} {'Diff':>12}")
    print("-" * 70)

    # Totals
    order_diff = keycrm_data["total_orders"] - dashboard_data["total_orders"]
    rev_diff = keycrm_data["total_revenue"] - dashboard_data["total_revenue"]
    ret_diff = keycrm_data["total_returns"] - dashboard_data["total_returns"]

    print(f"{'Orders':<20} {dashboard_data['total_orders']:>18,} {keycrm_data['total_orders']:>18,} {order_diff:>+12,}")
    print(f"{'Revenue':<20} {'₴{:,.2f}'.format(dashboard_data['total_revenue']):>18} {'₴{:,.2f}'.format(keycrm_data['total_revenue']):>18} {'₴{:+,.2f}'.format(rev_diff):>12}")
    print(f"{'Returns':<20} {dashboard_data['total_returns']:>18,} {keycrm_data['total_returns']:>18,} {ret_diff:>+12,}")

    # By source
    print(f"\n{'BY SOURCE':^70}")
    print("-" * 70)

    all_sources = set(dashboard_data["by_source"].keys()) | set(keycrm_data["by_source"].keys())

    for source in sorted(all_sources):
        db = dashboard_data["by_source"].get(source, {"orders": 0, "revenue": 0})
        kc = keycrm_data["by_source"].get(source, {"orders": 0, "revenue": 0, "returns": 0})

        print(f"\n{source}:")
        order_diff = kc["orders"] - db["orders"]
        rev_diff = kc["revenue"] - db["revenue"]

        status_orders = "✓" if order_diff == 0 else "✗"
        status_rev = "✓" if abs(rev_diff) < 100 else "✗"

        print(f"  {'Orders':<18} {db['orders']:>16,} {kc['orders']:>16,} {order_diff:>+10,} {status_orders}")
        print(f"  {'Revenue':<18} {'₴{:,.2f}'.format(db['revenue']):>16} {'₴{:,.2f}'.format(kc['revenue']):>16} {'₴{:+,.2f}'.format(rev_diff):>10} {status_rev}")
        if "returns" in kc:
            print(f"  {'Returns':<18} {'N/A':>16} {kc['returns']:>16,}")

    # Summary
    print(f"\n{'='*70}")
    total_order_diff = keycrm_data["total_orders"] - dashboard_data["total_orders"]
    total_rev_diff = keycrm_data["total_revenue"] - dashboard_data["total_revenue"]
    total_ret_diff = keycrm_data["total_returns"] - dashboard_data["total_returns"]

    if total_order_diff == 0 and abs(total_rev_diff) < 100 and total_ret_diff == 0:
        print("✓ Data matches within tolerance!")
    else:
        print("✗ Discrepancies found!")
        if total_order_diff != 0:
            print(f"  - Order count differs by {total_order_diff:+,}")
        if abs(total_rev_diff) >= 100:
            print(f"  - Revenue differs by ₴{total_rev_diff:+,.2f}")
        if total_ret_diff != 0:
            print(f"  - Return count differs by {total_ret_diff:+,}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python scripts/check_range_api.py START_DATE END_DATE")
        print("Example: python scripts/check_range_api.py 2026-01-16 2026-01-31")
        sys.exit(1)

    asyncio.run(main(sys.argv[1], sys.argv[2]))
