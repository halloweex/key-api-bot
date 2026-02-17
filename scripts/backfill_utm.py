#!/usr/bin/env python3
"""
Backfill manager_comment (UTM data) for existing orders in DuckDB.

Most orders were synced before the manager_comment column existed,
so they have NULL. This script re-fetches orders from KeyCRM API
and updates the manager_comment column, then refreshes UTM layers.

Usage:
    PYTHONPATH=. python scripts/backfill_utm.py
    PYTHONPATH=. python scripts/backfill_utm.py --days 90   # Only last 90 days

In Docker:
    docker exec keycrm-web python /app/scripts/backfill_utm.py
"""
import asyncio
import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DEFAULT_TZ = ZoneInfo("Europe/Kyiv")


async def backfill_utm(days_back: int = 730):
    from core.duckdb_store import get_store
    from core.keycrm import get_async_client

    store = await get_store()
    client = await get_async_client()

    # Count orders with NULL manager_comment
    async with store.connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM orders WHERE manager_comment IS NULL"
        ).fetchone()
        null_count = row[0]
        total = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        logger.info(f"Orders with NULL manager_comment: {null_count}/{total}")

    if null_count == 0:
        logger.info("All orders already have manager_comment, nothing to backfill")
        return

    # Fetch orders from API in chunks and update manager_comment
    final_end = datetime.now(DEFAULT_TZ) + timedelta(days=1)
    chunk_days = 90
    current_start = datetime.now(DEFAULT_TZ) - timedelta(days=days_back)
    updated_total = 0

    while current_start < final_end:
        current_end = min(current_start + timedelta(days=chunk_days), final_end)
        start_str = current_start.strftime('%Y-%m-%d')
        end_str = current_end.strftime('%Y-%m-%d')

        logger.info(f"Fetching orders {start_str} to {end_str}...")

        # Fetch minimal order data (just id + manager_comment)
        orders_by_id = {}
        params = {
            "filter[created_between]": f"{start_str}, {end_str}",
            "limit": 50,
        }
        try:
            async for batch in client.paginate("order", params=params, page_size=50):
                for order in batch:
                    mc = order.get("manager_comment")
                    if mc:
                        orders_by_id[order["id"]] = mc
        except Exception as e:
            logger.warning(f"Error fetching chunk {start_str}-{end_str}: {e}")
            current_start = current_end
            continue

        if orders_by_id:
            # Batch update manager_comment in DuckDB
            async with store.connection() as conn:
                conn.execute("BEGIN TRANSACTION")
                try:
                    for order_id, comment in orders_by_id.items():
                        conn.execute(
                            "UPDATE orders SET manager_comment = ? WHERE id = ? AND manager_comment IS NULL",
                            [comment, order_id]
                        )
                    conn.execute("COMMIT")
                    updated_total += len(orders_by_id)
                    logger.info(f"  Updated {len(orders_by_id)} orders with manager_comment")
                except Exception as e:
                    conn.execute("ROLLBACK")
                    logger.error(f"  Failed to update chunk: {e}")

        current_start = current_end

    logger.info(f"Backfill complete: updated {updated_total} orders")

    # Now refresh UTM layers
    logger.info("Clearing silver_order_utm to re-parse all comments...")
    async with store.connection() as conn:
        conn.execute("DELETE FROM silver_order_utm")

    logger.info("Refreshing UTM silver layer...")
    utm_count = await store.refresh_utm_silver_layer()
    logger.info(f"Parsed UTM for {utm_count} orders")

    logger.info("Refreshing traffic gold layer...")
    traffic_rows = await store.refresh_traffic_gold_layer()
    logger.info(f"Gold traffic layer: {traffic_rows} rows")

    # Show results
    async with store.connection() as conn:
        row = conn.execute(
            "SELECT traffic_type, platform, COUNT(*) FROM silver_order_utm "
            "GROUP BY traffic_type, platform ORDER BY COUNT(*) DESC LIMIT 15"
        ).fetchall()
        logger.info("UTM distribution:")
        for traffic_type, platform, count in row:
            logger.info(f"  {traffic_type:20} {platform:15} {count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill UTM data from KeyCRM")
    parser.add_argument("--days", type=int, default=730, help="Days of history to backfill (default: 730)")
    args = parser.parse_args()

    asyncio.run(backfill_utm(args.days))
