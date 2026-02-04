#!/usr/bin/env python3
"""
Find specific order discrepancies between Dashboard and KeyCRM API.
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


async def get_keycrm_orders(start_date: date, end_date: date, source_id: int = 1):
    """Get orders from KeyCRM API for date range and source."""
    client = KeyCRMClient()
    await client.connect()

    buffer_start = start_date - timedelta(days=1)
    buffer_end = end_date + timedelta(days=1)

    all_orders = []
    params = {
        "include": "products.offer,buyer",
        "filter[created_between]": f"{buffer_start}, {buffer_end}",
        "filter[source_id]": source_id,
    }

    async for batch in client.paginate("order", params=params, page_size=50):
        all_orders.extend(batch)

    # Filter to date range in Kyiv timezone
    target_orders = []
    for order in all_orders:
        ordered_at_str = order.get("ordered_at")
        if ordered_at_str:
            try:
                ordered_at = datetime.fromisoformat(ordered_at_str.replace("Z", "+00:00"))
                ordered_at_kyiv = ordered_at.astimezone(KYIV_TZ)
                if start_date <= ordered_at_kyiv.date() <= end_date:
                    target_orders.append({
                        "id": order["id"],
                        "ordered_at": ordered_at_kyiv.isoformat(),
                        "status_id": order.get("status_id"),
                        "grand_total": float(order.get("grand_total", 0)),
                        "buyer_id": order.get("buyer", {}).get("id") if order.get("buyer") else None,
                        "is_return": order.get("status_id") in RETURN_STATUSES
                    })
            except (ValueError, TypeError):
                pass

    await client.close()
    return target_orders


async def get_duckdb_orders(start_date: date, end_date: date, source_id: int = 1):
    """Get orders from DuckDB via direct query (read-only)."""
    import duckdb
    db_path = Path(__file__).parent.parent / "data" / "analytics.duckdb"

    try:
        conn = duckdb.connect(str(db_path), read_only=True)
    except Exception as e:
        print(f"Cannot connect to DuckDB: {e}")
        print("Using API fallback is not available for order-level data")
        return None

    results = conn.execute("""
        SELECT
            id, ordered_at, status_id, grand_total, buyer_id,
            status_id IN (19, 22, 21, 23) as is_return
        FROM orders
        WHERE DATE(ordered_at) BETWEEN ? AND ?
          AND source_id = ?
        ORDER BY id
    """, [start_date, end_date, source_id]).fetchall()

    conn.close()

    orders = []
    for row in results:
        orders.append({
            "id": row[0],
            "ordered_at": row[1].isoformat() if row[1] else None,
            "status_id": row[2],
            "grand_total": float(row[3]) if row[3] else 0,
            "buyer_id": row[4],
            "is_return": bool(row[5])
        })

    return orders


async def main():
    start_date = date(2026, 1, 16)
    end_date = date(2026, 1, 31)
    source_id = 1  # Instagram - where discrepancy was found

    print(f"\n{'='*70}")
    print(f"Finding Instagram order discrepancies: {start_date} to {end_date}")
    print(f"{'='*70}")

    print("\nFetching KeyCRM orders...")
    keycrm_orders = await get_keycrm_orders(start_date, end_date, source_id)
    print(f"  Found {len(keycrm_orders)} orders")

    print("\nFetching DuckDB orders...")
    duckdb_orders = await get_duckdb_orders(start_date, end_date, source_id)

    if duckdb_orders is None:
        print("  Cannot connect to DuckDB (locked)")
        return

    print(f"  Found {len(duckdb_orders)} orders")

    # Compare order IDs
    keycrm_ids = {o["id"] for o in keycrm_orders}
    duckdb_ids = {o["id"] for o in duckdb_orders}

    only_in_keycrm = keycrm_ids - duckdb_ids
    only_in_duckdb = duckdb_ids - keycrm_ids
    in_both = keycrm_ids & duckdb_ids

    print(f"\n{'COMPARISON':^70}")
    print("-" * 70)
    print(f"Orders only in KeyCRM: {len(only_in_keycrm)}")
    print(f"Orders only in DuckDB: {len(only_in_duckdb)}")
    print(f"Orders in both: {len(in_both)}")

    # Show details of missing orders
    if only_in_keycrm:
        print(f"\n{'Orders in KeyCRM but NOT in DuckDB:':^70}")
        print("-" * 70)
        for oid in sorted(only_in_keycrm):
            order = next(o for o in keycrm_orders if o["id"] == oid)
            print(f"  ID {oid}: {order['ordered_at']}, ₴{order['grand_total']:,.2f}, status={order['status_id']}, return={order['is_return']}")

    if only_in_duckdb:
        print(f"\n{'Orders in DuckDB but NOT in KeyCRM:':^70}")
        print("-" * 70)
        for oid in sorted(only_in_duckdb):
            order = next(o for o in duckdb_orders if o["id"] == oid)
            print(f"  ID {oid}: {order['ordered_at']}, ₴{order['grand_total']:,.2f}, status={order['status_id']}, return={order['is_return']}")

    # Check for status/amount differences in shared orders
    print(f"\n{'Checking for differences in shared orders...':^70}")
    differences = []
    for oid in in_both:
        kc = next(o for o in keycrm_orders if o["id"] == oid)
        db = next(o for o in duckdb_orders if o["id"] == oid)

        diffs = []
        if kc["status_id"] != db["status_id"]:
            diffs.append(f"status: KC={kc['status_id']} vs DB={db['status_id']}")
        if abs(kc["grand_total"] - db["grand_total"]) > 0.01:
            diffs.append(f"amount: KC=₴{kc['grand_total']:,.2f} vs DB=₴{db['grand_total']:,.2f}")
        if kc["is_return"] != db["is_return"]:
            diffs.append(f"is_return: KC={kc['is_return']} vs DB={db['is_return']}")

        if diffs:
            differences.append((oid, diffs))

    if differences:
        print("-" * 70)
        for oid, diffs in differences[:20]:  # Show first 20
            print(f"  ID {oid}: {', '.join(diffs)}")
        if len(differences) > 20:
            print(f"  ... and {len(differences) - 20} more")
    else:
        print("  No differences found in shared orders")

    # Summary
    print(f"\n{'='*70}")
    print("Summary:")
    print(f"  - KeyCRM non-return orders: {len([o for o in keycrm_orders if not o['is_return']])}")
    print(f"  - DuckDB non-return orders: {len([o for o in duckdb_orders if not o['is_return']])}")
    print(f"  - KeyCRM returns: {len([o for o in keycrm_orders if o['is_return']])}")
    print(f"  - DuckDB returns: {len([o for o in duckdb_orders if o['is_return']])}")

    # Revenue comparison
    kc_revenue = sum(o["grand_total"] for o in keycrm_orders if not o["is_return"])
    db_revenue = sum(o["grand_total"] for o in duckdb_orders if not o["is_return"])
    print(f"  - KeyCRM revenue: ₴{kc_revenue:,.2f}")
    print(f"  - DuckDB revenue: ₴{db_revenue:,.2f}")
    print(f"  - Difference: ₴{db_revenue - kc_revenue:,.2f}")


if __name__ == "__main__":
    asyncio.run(main())
