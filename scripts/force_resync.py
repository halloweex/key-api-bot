#!/usr/bin/env python3
"""
Force resync script to rebuild DuckDB from KeyCRM API.

Run this when you notice data discrepancies between dashboard and KeyCRM.

Usage:
    python scripts/force_resync.py
    python scripts/force_resync.py --days 90  # Only last 90 days
"""
import asyncio
import argparse
import logging
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.sync_service import force_resync, get_sync_service
from core.duckdb_store import get_store

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main(days_back: int = 365):
    """Run force resync."""
    logger.info(f"Starting force resync for last {days_back} days...")

    # Show current stats before resync
    store = await get_store()
    stats_before = await store.get_stats()
    logger.info(f"Before resync: {stats_before['orders']} orders, "
                f"date range: {stats_before['date_range']}")

    # Run force resync
    try:
        result = await force_resync(days_back=days_back)
        logger.info(f"Resync complete: {result}")

        # Show stats after resync
        stats_after = await store.get_stats()
        logger.info(f"After resync: {stats_after['orders']} orders, "
                    f"date range: {stats_after['date_range']}")

        # Show difference
        diff = stats_after['orders'] - stats_before['orders']
        if diff > 0:
            logger.info(f"Added {diff} new orders")
        elif diff < 0:
            logger.info(f"Removed {abs(diff)} duplicate/invalid orders")
        else:
            logger.info("Order count unchanged")

    except Exception as e:
        logger.error(f"Resync failed: {e}", exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Force resync DuckDB from KeyCRM")
    parser.add_argument(
        "--days",
        type=int,
        default=365,
        help="Number of days to sync (default: 365)"
    )
    args = parser.parse_args()

    exit_code = asyncio.run(main(days_back=args.days))
    sys.exit(exit_code)
