#!/usr/bin/env python3
"""
Check orders day-by-day to find when discrepancy occurs.
"""
import asyncio
import sys
import httpx
from datetime import datetime, date, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.keycrm import KeyCRMClient
from core.models import OrderStatus

KYIV_TZ = ZoneInfo("Europe/Kyiv")
RETURN_STATUSES = set(int(s) for s in OrderStatus.return_statuses())
SOURCE_NAMES = {1: "Instagram", 2: "Telegram", 4: "Shopify"}
API_BASE = "http://localhost:8080/api"


async def get_dashboard_day(target_date: date, source_id: int = None):
    """Get data from Dashboard API for a single day."""
    async with httpx.AsyncClient() as client:
        params = {
            "start_date": str(target_date),
            "end_date": str(target_date),
            "sales_type": "all"
        }
        if source_id:
            params["source_id"] = source_id

        summary_resp = await client.get(f"{API_BASE}/summary", params=params)
        summary = summary_resp.json()

        return {
            "orders": summary["totalOrders"],
            "revenue": float(summary["totalRevenue"]),
            "returns": summary["totalReturns"],
        }


async def get_keycrm_day(target_date: date, all_orders: list, source_id: int = None):
    """Filter pre-fetched KeyCRM orders for a specific day."""
    day_orders = []
    for order in all_orders:
        ordered_at_str = order.get("ordered_at")
        if ordered_at_str:
            try:
                ordered_at = datetime.fromisoformat(ordered_at_str.replace("Z", "+00:00"))
                ordered_at_kyiv = ordered_at.astimezone(KYIV_TZ)
                if ordered_at_kyiv.date() == target_date:
                    if source_id is None or order.get("source_id") == source_id:
                        day_orders.append(order)
            except (ValueError, TypeError):
                pass

    non_returns = [o for o in day_orders if o.get("status_id") not in RETURN_STATUSES]
    returns = [o for o in day_orders if o.get("status_id") in RETURN_STATUSES]

    return {
        "orders": len(non_returns),
        "revenue": sum(float(o.get("grand_total", 0)) for o in non_returns),
        "returns": len(returns),
    }


async def fetch_all_keycrm_orders(start_date: date, end_date: date):
    """Fetch all orders from KeyCRM for the date range."""
    client = KeyCRMClient()
    await client.connect()

    buffer_start = start_date - timedelta(days=1)
    buffer_end = end_date + timedelta(days=1)

    all_orders = []
    params = {
        "include": "products.offer",
        "filter[created_between]": f"{buffer_start}, {buffer_end}",
    }

    async for batch in client.paginate("order", params=params, page_size=50):
        all_orders.extend(batch)

    await client.close()

    # Filter to relevant sources
    return [o for o in all_orders if o.get("source_id") in (1, 2, 4)]


async def main():
    start_date = date(2026, 1, 16)
    end_date = date(2026, 1, 31)

    print(f"\n{'='*80}")
    print(f"Daily comparison: {start_date} to {end_date}")
    print(f"{'='*80}")

    # Fetch all KeyCRM orders once
    print("\nFetching all KeyCRM orders...")
    all_keycrm_orders = await fetch_all_keycrm_orders(start_date, end_date)
    print(f"  Fetched {len(all_keycrm_orders)} orders total\n")

    # Compare day by day
    print(f"{'Date':<12} {'Dashboard':^20} {'KeyCRM API':^20} {'Diff':^12} {'Status':<6}")
    print(f"{'':<12} {'Ord':>6} {'Rev':>13} {'Ord':>6} {'Rev':>13} {'Ord':>5} {'Rev':>6}")
    print("-" * 80)

    total_diff_orders = 0
    total_diff_revenue = 0
    problem_days = []

    current_date = start_date
    while current_date <= end_date:
        dashboard = await get_dashboard_day(current_date)
        keycrm = await get_keycrm_day(current_date, all_keycrm_orders)

        order_diff = dashboard["orders"] - keycrm["orders"]
        rev_diff = dashboard["revenue"] - keycrm["revenue"]

        total_diff_orders += order_diff
        total_diff_revenue += rev_diff

        status = "✓" if order_diff == 0 and abs(rev_diff) < 10 else "✗"
        if status == "✗":
            problem_days.append((current_date, order_diff, rev_diff))

        print(f"{current_date} {dashboard['orders']:>6} {dashboard['revenue']:>13,.2f} {keycrm['orders']:>6} {keycrm['revenue']:>13,.2f} {order_diff:>+5} {rev_diff:>+6,.0f} {status}")

        current_date += timedelta(days=1)

    print("-" * 80)
    print(f"{'TOTAL':<12} {'':<20} {'':<20} {total_diff_orders:>+5} {total_diff_revenue:>+6,.0f}")

    if problem_days:
        print(f"\n{'PROBLEM DAYS':^80}")
        print("-" * 80)
        for day, ord_diff, rev_diff in problem_days:
            print(f"  {day}: orders {ord_diff:+}, revenue ₴{rev_diff:+,.2f}")

        # Deep dive into first problem day - Instagram only
        print(f"\n{'DEEP DIVE - First problem day by source':^80}")
        print("-" * 80)
        problem_day = problem_days[0][0]

        for source_id, source_name in SOURCE_NAMES.items():
            dashboard = await get_dashboard_day(problem_day, source_id)
            keycrm = await get_keycrm_day(problem_day, all_keycrm_orders, source_id)

            order_diff = dashboard["orders"] - keycrm["orders"]
            rev_diff = dashboard["revenue"] - keycrm["revenue"]
            status = "✓" if order_diff == 0 and abs(rev_diff) < 10 else "✗"

            print(f"  {source_name:<12} Dashboard: {dashboard['orders']:>3} orders, ₴{dashboard['revenue']:>10,.2f}")
            print(f"  {'':<12} KeyCRM:    {keycrm['orders']:>3} orders, ₴{keycrm['revenue']:>10,.2f}")
            print(f"  {'':<12} Diff:      {order_diff:>+3} orders, ₴{rev_diff:>+10,.2f} {status}")
            print()


if __name__ == "__main__":
    asyncio.run(main())
