#!/usr/bin/env python3
"""
Backfill manager_comment (UTM data) for existing orders in DuckDB.

Most orders were synced before the manager_comment column existed,
so they have NULL. This script re-fetches orders from KeyCRM API
and updates the manager_comment column, then refreshes UTM layers.

It also ships every comment it restores to `bronze.orders`, because the daily
`mirror_landing` fingerprint compares `manager_comment` between the two stores,
and because step 9's Postgres UTM parser reads `bronze.orders.manager_comment`
directly. Under the default `KS_UTM_PARSE=duckdb` that is all it is for — the
/traffic tab reads `silver.order_utm`, which the re-parse below ships. Under
`KS_UTM_PARSE=postgres` it is also what the tab ends up showing: the re-parse
below then parses `silver.order_utm` in Postgres out of this copy.

**Stop the web container first** — see `WEB_MUST_BE_STOPPED` in
`core/pg_backfill.py`, which this logs at startup: the scheduler's heavy-job
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
    from core.duckdb_store import get_store
    from core.keycrm import get_async_client
    from core.pg_backfill import WEB_MUST_BE_STOPPED, ship_orders_by_id
    from core.pg_order_versions import BACKFILL
    from core.runtime_modes import configure_modes

    # Said out loud, once, at the top. The admin endpoint next door does this
    # work under the scheduler's heavy-job lock; this process has no such lock
    # to take, so the only thing that separates it from a sync tick is the
    # operator.
    logger.warning(WEB_MUST_BE_STOPPED)

    # Before the first write, because this script reaches a landing writer as
    # of DN-17 and did not before. `KS_PG_DERIVE` is read once and cached, and
    # in a process that never configured it `owns()` answers False — so every
    # order this ships would land unmarked and the derivation would not know
    # it owed a rebuild over the comments just restored.
    configure_modes()

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
    pg_shipped_total = 0
    # Orders changed in DuckDB that could not be shipped. Collected rather
    # than retried, for the reason at the catch below.
    pg_failed_ids: list[int] = []

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
            changed_ids = []
            async with store.connection() as conn:
                conn.execute("BEGIN TRANSACTION")
                try:
                    # The ids this chunk will actually change, read before the
                    # UPDATE. `COALESCE(EXCLUDED, stored)` on the Postgres side
                    # means an order whose comment did not move has nothing to
                    # carry, and the ship below needs exactly this list.
                    changed_ids = [int(r[0]) for r in conn.execute(
                        "SELECT id FROM orders WHERE id IN "
                        f"({', '.join('?' for _ in orders_by_id)}) "
                        "AND manager_comment IS NULL",
                        list(orders_by_id),
                    ).fetchall()]
                    for order_id in changed_ids:
                        conn.execute(
                            "UPDATE orders SET manager_comment = ? WHERE id = ? AND manager_comment IS NULL",
                            [orders_by_id[order_id], order_id]
                        )
                    conn.execute("COMMIT")
                    updated_total += len(changed_ids)
                    logger.info(f"  Updated {len(changed_ids)} orders with manager_comment")
                except Exception as e:
                    conn.execute("ROLLBACK")
                    changed_ids = []
                    logger.error(f"  Failed to update chunk: {e}")

            # And on to `bronze.orders`. Not for `/traffic` — that reads
            # `silver.order_utm`, which the re-parse at the end of this run
            # ships — but for the daily `mirror_landing` fingerprint, which
            # compares `manager_comment` between the stores, and for step 9's
            # Postgres parser, which reads this column. The hourly ids-diff
            # can never carry it: that ships the orders Postgres is *missing*,
            # and these exist on both sides and differ. Same helper the admin
            # endpoint calls, so the two backfills cannot drift;
            # `version_kind` is OD-20 (b). No lock is passed — there is none
            # to pass from a separate process, which is what
            # `WEB_MUST_BE_STOPPED` above is about.
            #
            # Caught per chunk. The helper raises by contract, and that
            # contract was written for `backfill_orders`, which has no second
            # job to lose. This run does: the DuckDB re-parse below was
            # Postgres-independent before DN-17 and must stay so, or an
            # unreachable Postgres costs an operator the whole restore — and
            # the SELECT above only offers rows whose comment is still NULL,
            # so a re-run never re-offers what this chunk just changed.
            try:
                shipped = await ship_orders_by_id(
                    store, changed_ids, version_kind=BACKFILL,
                )
                pg_shipped_total += shipped["orders_shipped"]
            except Exception as ship_error:
                pg_failed_ids.extend(changed_ids)
                logger.error(
                    "  Failed to ship %d order(s) to Postgres, carrying on: "
                    "%s", len(changed_ids), ship_error,
                )

        current_start = current_end

    logger.info(
        f"Backfill complete: updated {updated_total} orders, "
        f"shipped {pg_shipped_total} to Postgres"
    )
    if pg_failed_ids:
        # Named, because nothing else will name them: a re-run skips these
        # rows and the daily reconciliation reports them a bucket at a time.
        logger.error(
            "%d order(s) were restored in DuckDB and never reached Postgres. "
            "Ids: %s", len(pg_failed_ids), pg_failed_ids,
        )

    # Now refresh UTM layers
    logger.info("Clearing silver_order_utm to re-parse all comments...")
    async with store.connection() as conn:
        conn.execute("DELETE FROM silver_order_utm")

    logger.info("Refreshing UTM silver layer...")
    utm_count = len(await store.refresh_utm_silver_layer())
    logger.info(f"Parsed UTM for {utm_count} orders")

    logger.info("Refreshing traffic gold layer...")

    # And on to Postgres, which is what `/traffic` reads since revision 0018.
    # This script rewrites `silver_order_utm` without marking the warehouse
    # dirty, so without this the operator who just ran it would look at the
    # tab and see the classification they replaced. The three admin endpoints
    # that do the same thing call the same router; never raises. `full`,
    # because everything was deleted and re-parsed above: under
    # KS_UTM_PARSE=postgres the table is re-parsed whole out of
    # `bronze.orders`, and under the default DuckDB's copy is shipped.
    #
    # Either way it refuses a result under 90% of what Postgres holds — and
    # the ship one made while the last DuckDB parse is recorded as failed —
    # and logs the refusal rather than raising, so read the line below.
    # `--force-ship` is the only way past that guard anywhere in the
    # repository, and it is here, behind a flag an operator types, because
    # this is the one caller that may mean a shrink.
    from core.pg_utm_parse import reparse_router

    logger.info(
        "UTM to Postgres: %s",
        await reparse_router(store, full=True, force=force_ship),
    )

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
    # The warning is logged at startup too, but by then the operator has
    # already typed the command; `--help` is where they are still deciding.
    from core.pg_backfill import WEB_MUST_BE_STOPPED

    parser = argparse.ArgumentParser(
        description="Backfill UTM data from KeyCRM",
        epilog=WEB_MUST_BE_STOPPED,
    )
    parser.add_argument("--days", type=int, default=730, help="Days of history to backfill (default: 730)")
    parser.add_argument(
        "--force-ship", action="store_true",
        help=(
            "Replace Postgres' silver.order_utm even when the new table holds "
            "under 90%% of its rows (DuckDB's copy, or under KS_UTM_PARSE=postgres "
            "the parse of bronze.orders), or the last DuckDB parse failed. "
            "/traffic reads that table: use only when the smaller table is the "
            "one you mean."
        ),
    )
    args = parser.parse_args()

    asyncio.run(backfill_utm(args.days, force_ship=args.force_ship))
