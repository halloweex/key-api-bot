#!/usr/bin/env python3
"""
Backfill manager_comment (UTM data) for existing orders, from KeyCRM.

Many orders were synced before the manager_comment column existed, or lost
their comment to the July 2026 template break, so they hold NULL. This script
re-fetches orders from KeyCRM and fills in the comment where Postgres holds it
as NULL (`pg_orders_write.restore_manager_comments`, archived as
`kind='backfill'`, OD-20 (b)), then re-parses the UTM verdicts in Postgres,
which is what the /traffic tab reads.

Chain 3 writes the orders to Postgres alone. Until step 14's S14-09 this
script asked `duckdb_store._orders_in_postgres()` first — a constant since
S14-00 — and kept a DuckDB walk (an UPDATE of `orders` and a ship of the
changed rows to `bronze.orders`) for the answer it no longer gave.

**Stop the web container first** — see `WEB_MUST_BE_STOPPED` in
`core/pg_orders_write.py`, which this logs at startup: the scheduler's heavy-job
lock lives in that process and cannot be taken from here.

Usage:
    PYTHONPATH=. python scripts/backfill_utm.py
    PYTHONPATH=. python scripts/backfill_utm.py --days 90   # Only last 90 days
    PYTHONPATH=. python scripts/backfill_utm.py --force-ship  # Replace even with a shrink

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


async def backfill_utm(days_back: int = 730, force_ship: bool = False):
    from core.pg_orders_write import WEB_MUST_BE_STOPPED
    from core.runtime_modes import configure_modes

    # Said out loud, once, at the top. The admin endpoint next door does this
    # work under the scheduler's heavy-job lock; this process has no such lock
    # to take, so the only thing that separates it from a sync tick is the
    # operator.
    logger.warning(WEB_MUST_BE_STOPPED)

    # Before the first write: the latch is read once and cached, and the
    # chain's writer must find it loaded.
    configure_modes()

    return await _backfill_utm_postgres(days_back, force_ship=force_ship)


async def _reparse(*, force_ship: bool) -> None:
    """The re-parse the backfill ends on, and the one place this script hands
    `--force-ship` on (`tests/unit/test_utm_parse_doors.py` holds it to
    exactly one).

    `full`, because the comments restored above may change any verdict: the
    table is re-parsed whole out of `bronze.orders`, in Postgres. It refuses a
    result under 90% of what Postgres holds, and a parse whose verdict log did
    not first keep what the table holds (owner decision D12:
    `KS_ANALYTICS_CAPTURES=off`, or the capture failing), and logs the refusal
    rather than raising, so read the line below. `--force-ship` is the only
    way past either guard anywhere in the repository, and it is here, behind a
    flag an operator types, because this is the one caller that may mean a
    shrink.

    Until step 14's S14-15 it re-parsed DuckDB's `silver_order_utm` first while
    DuckDB derived, and under step 13 noted the Postgres-only re-parse for a
    way back (DuckDB's recorded writer, or `data/warehouse-utm-reparsed-at`
    under KS_DUCKDB=off). Both went with step 13's switch; the marker already
    on disk is left as it is."""
    from core.pg_utm_parse import reparse_router

    logger.info(
        "UTM to Postgres: %s",
        await reparse_router(full=True, force=force_ship),
    )


async def _backfill_utm_postgres(days_back: int, *, force_ship: bool = False):
    """`backfill_utm`'s body, chain 3's (`core/pg_orders_write.py`): a walk
    over KeyCRM, each chunk's comments written by
    `pg_orders_write.restore_manager_comments` — only where Postgres holds the
    comment as NULL, archived as `kind='backfill'` (OD-20 (b)) — and nothing
    written to DuckDB, whose orders the chain froze. Step 13 is in force, so
    Postgres alone re-parses (`reparse_router`, full). Web must be stopped
    (`WEB_MUST_BE_STOPPED`, logged by `backfill_utm`, its one caller)."""
    from core import pg_orders_read, pg_orders_write
    from core.keycrm import get_async_client
    from core.runtime_modes import configure_modes

    # Idempotent, and asked again here because this function is the one that
    # writes: the latch must be loaded before the chain's writer runs,
    # whoever called it.
    configure_modes()
    null_count = await pg_orders_read.null_comment_count()
    logger.info(f"Orders with NULL manager_comment in Postgres: {null_count}")
    if null_count == 0:
        logger.info("All orders already have manager_comment, nothing to backfill")
        return

    client = await get_async_client()
    final_end = datetime.now(DEFAULT_TZ) + timedelta(days=1)
    current_start = datetime.now(DEFAULT_TZ) - timedelta(days=days_back)
    restored_total = 0
    failed_chunks: list[str] = []
    while current_start < final_end:
        current_end = min(current_start + timedelta(days=90), final_end)
        start_str, end_str = current_start.strftime('%Y-%m-%d'), current_end.strftime('%Y-%m-%d')
        logger.info(f"Fetching orders {start_str} to {end_str}...")
        comments = {}
        params = {"filter[created_between]": f"{start_str}, {end_str}", "limit": 50}
        try:
            async for batch in client.paginate("order", params=params, page_size=50):
                for order in batch:
                    if order.get("manager_comment"):
                        comments[order["id"]] = order["manager_comment"]
        except Exception as e:
            logger.warning(f"Error fetching chunk {start_str}-{end_str}: {e}")
            current_start = current_end
            continue
        if comments:
            try:
                restored = await pg_orders_write.restore_manager_comments(comments)
                restored_total += len(restored)
                logger.info(f"  Restored {len(restored)} comment(s) in Postgres")
            except Exception as write_error:
                failed_chunks.append(f"{start_str}..{end_str}")
                logger.error("  Chunk %s..%s not written: %s", start_str, end_str,
                             type(write_error).__name__)
        current_start = current_end

    logger.info(f"Backfill complete: restored {restored_total} comment(s) in Postgres")
    if failed_chunks:
        logger.error("Chunks not written, offered again by a re-run: %s", failed_chunks)
    await _reparse(force_ship=force_ship)


if __name__ == "__main__":
    # The warning is logged at startup too, but by then the operator has
    # already typed the command; `--help` is where they are still deciding.
    from core.pg_orders_write import WEB_MUST_BE_STOPPED

    parser = argparse.ArgumentParser(
        description="Backfill UTM data from KeyCRM",
        epilog=WEB_MUST_BE_STOPPED,
    )
    parser.add_argument("--days", type=int, default=730, help="Days of history to backfill (default: 730)")
    parser.add_argument(
        "--force-ship", action="store_true",
        help=(
            "Replace Postgres' silver.order_utm even when the new table holds "
            "under 90%% of its rows (the parse of bronze.orders). /traffic "
            "reads that table: use only when the smaller table is the one you "
            "mean."
        ),
    )
    args = parser.parse_args()

    asyncio.run(backfill_utm(args.days, force_ship=args.force_ship))
