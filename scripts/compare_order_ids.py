#!/usr/bin/env python3
"""
Find specific order ID differences between DuckDB and KeyCRM API for a given date.

Usage:
    PYTHONPATH=. python scripts/compare_order_ids.py 2026-04-07
"""
import asyncio
import argparse
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.duckdb_store import get_store
from core.keycrm import get_async_client
from core.models import OrderStatus

KYIV_TZ = ZoneInfo("Europe/Kyiv")
RETURN_STATUSES = {int(s) for s in OrderStatus.return_statuses()}


async def main(target_date: date):
    store = await get_store()
    client = await get_async_client()

    # ── DB orders ──
    async with store.connection() as conn:
        db_rows = conn.execute("""
            SELECT id, source_id, status_id, grand_total, ordered_at, created_at, manager_id
            FROM orders
            WHERE DATE(timezone('Europe/Kyiv', ordered_at)) = ?
              AND source_id IN (1, 2, 4)
            ORDER BY id
        """, [target_date]).fetchall()

    db_orders = {}
    for r in db_rows:
        db_orders[r[0]] = {
            "source_id": r[1], "status_id": r[2], "grand_total": float(r[3]),
            "ordered_at": r[4], "created_at": r[5], "manager_id": r[6],
        }

    # ── API orders ──
    # Fetch with ±1 day window to catch TZ edge cases
    api_start = (target_date - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    api_end = (target_date + timedelta(days=1)).strftime("%Y-%m-%d 23:59:59")

    all_api = {}
    params = {"filter[created_between]": f"{api_start}, {api_end}"}
    async for batch in client.paginate("order", params=params, page_size=50):
        for order in batch:
            all_api[order["id"]] = order

    # Filter to target_date by ordered_at in Kyiv TZ
    api_orders = {}
    for oid, order in all_api.items():
        if order.get("source_id") not in (1, 2, 4):
            continue
        ordered_at_str = order.get("ordered_at")
        if not ordered_at_str:
            continue
        try:
            dt = datetime.fromisoformat(ordered_at_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=ZoneInfo("Etc/GMT-4"))
            dt_kyiv = dt.astimezone(KYIV_TZ)
            if dt_kyiv.date() == target_date:
                api_orders[oid] = {
                    "source_id": order.get("source_id"),
                    "status_id": order.get("status_id"),
                    "grand_total": float(order.get("grand_total", 0)),
                    "ordered_at": ordered_at_str,
                    "created_at": order.get("created_at"),
                    "manager_id": order.get("manager_id"),
                }
        except (ValueError, TypeError):
            continue

    db_ids = set(db_orders.keys())
    api_ids = set(api_orders.keys())
    only_db = sorted(db_ids - api_ids)
    only_api = sorted(api_ids - db_ids)
    both = sorted(db_ids & api_ids)

    print(f"\n=== Order comparison for {target_date} ===")
    print(f"DB orders:  {len(db_orders)} (non-return: {sum(1 for o in db_orders.values() if o['status_id'] not in RETURN_STATUSES)})")
    print(f"API orders: {len(api_orders)} (non-return: {sum(1 for o in api_orders.values() if o['status_id'] not in RETURN_STATUSES)})")
    print(f"In both:    {len(both)}")

    if only_db:
        print(f"\n── Only in DB ({len(only_db)}) ──")
        for oid in only_db:
            o = db_orders[oid]
            ret = " [RETURN]" if o["status_id"] in RETURN_STATUSES else ""
            print(f"  #{oid}  src={o['source_id']}  status={o['status_id']}{ret}  "
                  f"₴{o['grand_total']:,.2f}  ordered_at={o['ordered_at']}  mgr={o['manager_id']}")

    if only_api:
        print(f"\n── Only in API ({len(only_api)}) ──")
        for oid in only_api:
            o = api_orders[oid]
            ret = " [RETURN]" if o["status_id"] in RETURN_STATUSES else ""
            print(f"  #{oid}  src={o['source_id']}  status={o['status_id']}{ret}  "
                  f"₴{o['grand_total']:,.2f}  ordered_at={o['ordered_at']}  mgr={o['manager_id']}")

    # Check for status or revenue mismatches in shared orders
    mismatches = []
    for oid in both:
        db = db_orders[oid]
        api = api_orders[oid]
        diffs = []
        if db["status_id"] != api["status_id"]:
            diffs.append(f"status DB={db['status_id']} API={api['status_id']}")
        if abs(db["grand_total"] - api["grand_total"]) > 0.01:
            diffs.append(f"total DB=₴{db['grand_total']:,.2f} API=₴{api['grand_total']:,.2f}")
        if diffs:
            mismatches.append((oid, diffs))

    if mismatches:
        print(f"\n── Mismatches in shared orders ({len(mismatches)}) ──")
        for oid, diffs in mismatches:
            print(f"  #{oid}  {', '.join(diffs)}")
    else:
        print("\n── No mismatches in shared orders ──")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("date", help="Date to check (YYYY-MM-DD)")
    args = parser.parse_args()
    asyncio.run(main(date.fromisoformat(args.date)))
