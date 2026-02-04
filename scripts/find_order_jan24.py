#!/usr/bin/env python3
"""
Find the specific Instagram order on 2026-01-24 causing discrepancy.
"""
import asyncio
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.keycrm import KeyCRMClient
from core.models import OrderStatus

KYIV_TZ = ZoneInfo("Europe/Kyiv")
RETURN_STATUSES = set(int(s) for s in OrderStatus.return_statuses())


async def main():
    target_date = date(2026, 1, 24)
    print(f"\n{'='*80}")
    print(f"Finding Instagram orders on {target_date}")
    print(f"{'='*80}")

    client = KeyCRMClient()
    await client.connect()

    # Fetch orders around Jan 24 for Instagram (source_id=1)
    buffer_start = target_date - timedelta(days=2)
    buffer_end = target_date + timedelta(days=2)

    all_orders = []
    params = {
        "include": "products.offer,buyer",
        "filter[created_between]": f"{buffer_start}, {buffer_end}",
        "filter[source_id]": 1,  # Instagram only
    }

    print(f"\nFetching Instagram orders from {buffer_start} to {buffer_end}...")
    async for batch in client.paginate("order", params=params, page_size=50):
        all_orders.extend(batch)
    print(f"  Fetched {len(all_orders)} Instagram orders")

    # Organize by date (Kyiv timezone)
    by_date = {}
    for order in all_orders:
        ordered_at_str = order.get("ordered_at")
        if ordered_at_str:
            try:
                ordered_at = datetime.fromisoformat(ordered_at_str.replace("Z", "+00:00"))
                ordered_at_kyiv = ordered_at.astimezone(KYIV_TZ)
                order_date = ordered_at_kyiv.date()

                if order_date not in by_date:
                    by_date[order_date] = []
                by_date[order_date].append({
                    "id": order["id"],
                    "ordered_at_raw": ordered_at_str,
                    "ordered_at_kyiv": ordered_at_kyiv.isoformat(),
                    "status_id": order.get("status_id"),
                    "grand_total": float(order.get("grand_total", 0)),
                    "is_return": order.get("status_id") in RETURN_STATUSES,
                    "buyer_id": order.get("buyer", {}).get("id") if order.get("buyer") else None,
                })
            except (ValueError, TypeError):
                pass

    # Print orders by date
    for d in sorted(by_date.keys()):
        orders = by_date[d]
        non_returns = [o for o in orders if not o["is_return"]]
        returns = [o for o in orders if o["is_return"]]
        revenue = sum(o["grand_total"] for o in non_returns)

        marker = " <-- PROBLEM DAY" if d == target_date else ""
        print(f"\n{d}: {len(non_returns)} orders, {len(returns)} returns, ₴{revenue:,.2f}{marker}")

        if d == target_date:
            print("  Orders (non-return):")
            for o in sorted(non_returns, key=lambda x: x["grand_total"], reverse=True):
                print(f"    ID {o['id']}: ₴{o['grand_total']:,.2f}, ordered_at={o['ordered_at_kyiv']}")

    # Check for orders near midnight that might be on wrong day
    print(f"\n{'='*80}")
    print("Checking for timezone edge cases (orders near midnight):")
    print("-" * 80)

    jan23_orders = by_date.get(date(2026, 1, 23), [])
    jan24_orders = by_date.get(date(2026, 1, 24), [])
    jan25_orders = by_date.get(date(2026, 1, 25), [])

    # Check Jan 23 late night orders (could be Jan 24 in some timezones)
    late_jan23 = [o for o in jan23_orders if "23:00" <= o["ordered_at_kyiv"][11:16] <= "23:59"]
    if late_jan23:
        print(f"\nLate Jan 23 orders (23:00-23:59 Kyiv):")
        for o in late_jan23:
            print(f"  ID {o['id']}: {o['ordered_at_kyiv']}, ₴{o['grand_total']:,.2f}")

    # Check Jan 24 early morning orders (could be Jan 23 in some timezones)
    early_jan24 = [o for o in jan24_orders if "00:00" <= o["ordered_at_kyiv"][11:16] <= "02:00"]
    if early_jan24:
        print(f"\nEarly Jan 24 orders (00:00-02:00 Kyiv):")
        for o in early_jan24:
            print(f"  ID {o['id']}: {o['ordered_at_kyiv']}, raw={o['ordered_at_raw']}, ₴{o['grand_total']:,.2f}")

    # Check Jan 24 late night orders
    late_jan24 = [o for o in jan24_orders if "21:00" <= o["ordered_at_kyiv"][11:16] <= "23:59"]
    if late_jan24:
        print(f"\nLate Jan 24 orders (21:00-23:59 Kyiv):")
        for o in late_jan24:
            print(f"  ID {o['id']}: {o['ordered_at_kyiv']}, raw={o['ordered_at_raw']}, ₴{o['grand_total']:,.2f}")

    # Check Jan 25 early morning orders
    early_jan25 = [o for o in jan25_orders if "00:00" <= o["ordered_at_kyiv"][11:16] <= "02:00"]
    if early_jan25:
        print(f"\nEarly Jan 25 orders (00:00-02:00 Kyiv):")
        for o in early_jan25:
            print(f"  ID {o['id']}: {o['ordered_at_kyiv']}, raw={o['ordered_at_raw']}, ₴{o['grand_total']:,.2f}")

    # Find the ₴2,358 order
    print(f"\n{'='*80}")
    print("Looking for ₴2,358 order (the discrepancy amount):")
    print("-" * 80)

    for d, orders in by_date.items():
        for o in orders:
            if abs(o["grand_total"] - 2358) < 1:
                print(f"  Found! ID {o['id']}: date={d}, ₴{o['grand_total']:,.2f}")
                print(f"         ordered_at_kyiv: {o['ordered_at_kyiv']}")
                print(f"         ordered_at_raw:  {o['ordered_at_raw']}")
                print(f"         is_return: {o['is_return']}")

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
