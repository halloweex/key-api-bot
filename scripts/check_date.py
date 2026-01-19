#!/usr/bin/env python3
"""
Check orders for a specific date in DuckDB and compare with KeyCRM API.

Usage:
    python scripts/check_date.py 2025-12-07
"""
import asyncio
import argparse
import logging
import sys
from datetime import datetime, date
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.duckdb_store import get_store
from core.keycrm import get_async_client
from core.models import OrderStatus

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def check_duckdb(target_date: date):
    """Check orders in DuckDB for the given date."""
    store = await get_store()
    return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())

    async with store.connection() as conn:
        # Get all orders for the date
        results = conn.execute("""
            SELECT
                id, source_id, status_id, grand_total,
                ordered_at, created_at, manager_id
            FROM orders
            WHERE DATE(ordered_at) = ?
            ORDER BY id
        """, [target_date]).fetchall()

        total_orders = len(results)
        non_return_orders = [r for r in results if r[2] not in return_statuses]
        total_revenue = sum(r[3] for r in non_return_orders)

        # By source
        source_names = {1: "Instagram", 2: "Telegram", 4: "Shopify"}
        by_source = {}
        for r in non_return_orders:
            sid = r[1]
            name = source_names.get(sid, f"Source {sid}")
            if name not in by_source:
                by_source[name] = {"count": 0, "revenue": 0}
            by_source[name]["count"] += 1
            by_source[name]["revenue"] += r[3]

        return {
            "total_orders": total_orders,
            "non_return_orders": len(non_return_orders),
            "total_revenue": float(total_revenue),
            "by_source": by_source,
            "return_count": total_orders - len(non_return_orders)
        }


async def check_keycrm(target_date: date):
    """Check orders in KeyCRM API for the given date."""
    client = await get_async_client()
    return_statuses = tuple(int(s) for s in OrderStatus.return_statuses())

    # Use created_between with a wide range to capture all orders
    # Then filter by ordered_at locally
    start_str = (target_date.replace(day=1)).strftime('%Y-%m-%d')
    end_str = target_date.strftime('%Y-%m-%d 23:59:59')

    # Fetch orders created in the month containing the target date
    orders = []
    params = {
        "include": "products.offer,manager,buyer",
        "filter[created_between]": f"{start_str}, {end_str}",
    }

    async for batch in client.paginate("order", params=params, page_size=50):
        orders.extend(batch)

    # Also try updated_between to catch any orders that were updated
    params_updated = {
        "include": "products.offer,manager,buyer",
        "filter[updated_between]": f"{start_str}, {end_str}",
    }
    try:
        async for batch in client.paginate("order", params=params_updated, page_size=50):
            for order in batch:
                if order["id"] not in [o["id"] for o in orders]:
                    orders.append(order)
    except Exception:
        pass

    # Filter to only orders with ordered_at on target date
    target_orders = []
    for order in orders:
        ordered_at_str = order.get("ordered_at")
        if ordered_at_str:
            try:
                ordered_at = datetime.fromisoformat(ordered_at_str.replace("Z", "+00:00"))
                if ordered_at.date() == target_date:
                    target_orders.append(order)
            except (ValueError, TypeError):
                pass

    # Calculate stats
    non_return_orders = [o for o in target_orders if o.get("status_id") not in return_statuses]
    total_revenue = sum(float(o.get("grand_total", 0)) for o in non_return_orders)

    # By source
    source_names = {1: "Instagram", 2: "Telegram", 4: "Shopify"}
    by_source = {}
    for o in non_return_orders:
        sid = o.get("source_id")
        name = source_names.get(sid, f"Source {sid}")
        if name not in by_source:
            by_source[name] = {"count": 0, "revenue": 0}
        by_source[name]["count"] += 1
        by_source[name]["revenue"] += float(o.get("grand_total", 0))

    return {
        "total_orders": len(target_orders),
        "non_return_orders": len(non_return_orders),
        "total_revenue": total_revenue,
        "by_source": by_source,
        "return_count": len(target_orders) - len(non_return_orders)
    }


async def main(date_str: str):
    """Compare DuckDB and KeyCRM data for a date."""
    target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    logger.info(f"Checking data for {target_date}")

    # Check DuckDB
    logger.info("\n=== DuckDB ===")
    duckdb_data = await check_duckdb(target_date)
    logger.info(f"Total orders: {duckdb_data['total_orders']}")
    logger.info(f"Non-return orders: {duckdb_data['non_return_orders']}")
    logger.info(f"Total revenue: ₴{duckdb_data['total_revenue']:,.2f}")
    logger.info(f"Returns: {duckdb_data['return_count']}")
    for source, data in duckdb_data['by_source'].items():
        logger.info(f"  {source}: {data['count']} orders, ₴{data['revenue']:,.2f}")

    # Check KeyCRM
    logger.info("\n=== KeyCRM API ===")
    keycrm_data = await check_keycrm(target_date)
    logger.info(f"Total orders: {keycrm_data['total_orders']}")
    logger.info(f"Non-return orders: {keycrm_data['non_return_orders']}")
    logger.info(f"Total revenue: ₴{keycrm_data['total_revenue']:,.2f}")
    logger.info(f"Returns: {keycrm_data['return_count']}")
    for source, data in keycrm_data['by_source'].items():
        logger.info(f"  {source}: {data['count']} orders, ₴{data['revenue']:,.2f}")

    # Comparison
    logger.info("\n=== Comparison ===")
    order_diff = keycrm_data['non_return_orders'] - duckdb_data['non_return_orders']
    revenue_diff = keycrm_data['total_revenue'] - duckdb_data['total_revenue']

    if order_diff == 0 and abs(revenue_diff) < 1:
        logger.info("✓ Data matches!")
    else:
        logger.warning(f"✗ Order difference: {order_diff}")
        logger.warning(f"✗ Revenue difference: ₴{revenue_diff:,.2f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check orders for a specific date")
    parser.add_argument("date", help="Date to check (YYYY-MM-DD)")
    args = parser.parse_args()

    asyncio.run(main(args.date))
