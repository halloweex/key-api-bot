#!/usr/bin/env python3
"""
Compare orders and revenue between DuckDB and KeyCRM API per day.

Fetches all orders from API for the date range, converts ordered_at to
Europe/Kyiv timezone, groups by date, and compares with DuckDB.

Usage:
    PYTHONPATH=. python scripts/compare_orders_revenue.py [--days 14]
"""
import asyncio
import argparse
import logging
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.duckdb_store import get_store
from core.keycrm import get_async_client
from core.models import OrderStatus

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

KYIV_TZ = ZoneInfo("Europe/Kyiv")
RETURN_STATUSES = {int(s) for s in OrderStatus.return_statuses()}


async def get_db_daily(days_back: int) -> dict[date, dict]:
    """Query DuckDB for per-day order count and revenue (ordered_at in Kyiv TZ)."""
    store = await get_store()
    today = date.today()
    start = today - timedelta(days=days_back)

    async with store.connection() as conn:
        rows = conn.execute("""
            SELECT
                DATE(timezone('Europe/Kyiv', ordered_at)) AS d,
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE status_id NOT IN (19, 21, 22, 23)) AS non_return,
                COALESCE(SUM(grand_total) FILTER (WHERE status_id NOT IN (19, 21, 22, 23)), 0) AS revenue,
                COUNT(*) FILTER (WHERE status_id IN (19, 21, 22, 23)) AS returns
            FROM orders
            WHERE DATE(timezone('Europe/Kyiv', ordered_at)) BETWEEN ? AND ?
              AND source_id IN (1, 2, 4)
            GROUP BY d
            ORDER BY d
        """, [start, today - timedelta(days=1)]).fetchall()

    return {
        row[0]: {
            "total": row[1],
            "non_return": row[2],
            "revenue": float(row[3]),
            "returns": row[4],
        }
        for row in rows
    }


async def get_api_daily(days_back: int) -> dict[date, dict]:
    """Fetch orders from KeyCRM API, parse ordered_at in Kyiv TZ, group by date."""
    client = await get_async_client()
    today = date.today()
    start = today - timedelta(days=days_back)

    # Widen the window by 30 days to catch backdated B2B orders
    api_start = (start - timedelta(days=30)).strftime("%Y-%m-%d 00:00:00")
    api_end = today.strftime("%Y-%m-%d 23:59:59")

    logger.info(f"Fetching API orders created between {api_start} and {api_end} ...")

    orders_by_id = {}
    params = {
        "filter[created_between]": f"{api_start}, {api_end}",
    }
    page_count = 0
    async for batch in client.paginate("order", params=params, page_size=50):
        for order in batch:
            orders_by_id[order["id"]] = order
        page_count += 1
        if page_count % 20 == 0:
            logger.info(f"  ... {len(orders_by_id)} orders fetched so far")

    logger.info(f"Total API orders fetched: {len(orders_by_id)}")

    # Group by ordered_at date in Kyiv TZ
    daily: dict[date, dict] = defaultdict(lambda: {"total": 0, "non_return": 0, "revenue": 0.0, "returns": 0})

    for order in orders_by_id.values():
        source_id = order.get("source_id")
        if source_id not in (1, 2, 4):
            continue

        ordered_at_str = order.get("ordered_at")
        if not ordered_at_str:
            continue

        try:
            # KeyCRM returns timestamps like "2026-04-15 14:30:00"
            # These are in server TZ (+04:00)
            dt = datetime.fromisoformat(ordered_at_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=ZoneInfo("Etc/GMT-4"))  # KeyCRM server = UTC+4
            dt_kyiv = dt.astimezone(KYIV_TZ)
            d = dt_kyiv.date()
        except (ValueError, TypeError):
            continue

        if d < start or d >= today:
            continue

        status_id = order.get("status_id")
        grand_total = float(order.get("grand_total", 0))

        daily[d]["total"] += 1
        if status_id in RETURN_STATUSES:
            daily[d]["returns"] += 1
        else:
            daily[d]["non_return"] += 1
            daily[d]["revenue"] += grand_total

    return dict(daily)


async def main(days_back: int):
    logger.info(f"Comparing last {days_back} days (DB vs API)")

    db_data, api_data = await asyncio.gather(
        get_db_daily(days_back),
        get_api_daily(days_back),
    )

    today = date.today()
    all_dates = sorted(
        {d for d in db_data} | {d for d in api_data}
    )

    # Print header
    print()
    print(f"{'Date':<12} {'DB Ord':>7} {'API Ord':>8} {'Δ Ord':>6}  "
          f"{'DB Rev':>12} {'API Rev':>12} {'Δ Rev':>10}  {'DB Ret':>6} {'API Ret':>7}")
    print("─" * 95)

    total_db_orders = 0
    total_api_orders = 0
    total_db_rev = 0.0
    total_api_rev = 0.0
    drift_days = 0

    for d in all_dates:
        db = db_data.get(d, {"total": 0, "non_return": 0, "revenue": 0.0, "returns": 0})
        api = api_data.get(d, {"total": 0, "non_return": 0, "revenue": 0.0, "returns": 0})

        d_ord = api["non_return"] - db["non_return"]
        d_rev = api["revenue"] - db["revenue"]

        flag = " ✗" if abs(d_ord) > 0 or abs(d_rev) > 1 else " ✓"

        print(
            f"{d.isoformat():<12} {db['non_return']:>7} {api['non_return']:>8} {d_ord:>+6}  "
            f"₴{db['revenue']:>11,.2f} ₴{api['revenue']:>11,.2f} {d_rev:>+10,.2f}  "
            f"{db['returns']:>6} {api['returns']:>7}{flag}"
        )

        total_db_orders += db["non_return"]
        total_api_orders += api["non_return"]
        total_db_rev += db["revenue"]
        total_api_rev += api["revenue"]
        if abs(d_ord) > 0:
            drift_days += 1

    print("─" * 95)
    print(
        f"{'TOTAL':<12} {total_db_orders:>7} {total_api_orders:>8} {total_api_orders - total_db_orders:>+6}  "
        f"₴{total_db_rev:>11,.2f} ₴{total_api_rev:>11,.2f} {total_api_rev - total_db_rev:>+10,.2f}  "
        f"{'':>6} {'':>7}"
    )
    print(f"\nDays with order drift: {drift_days}/{len(all_dates)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare orders and revenue: DuckDB vs KeyCRM API")
    parser.add_argument("--days", type=int, default=14, help="Days back to check (default: 14)")
    args = parser.parse_args()
    asyncio.run(main(args.days))
