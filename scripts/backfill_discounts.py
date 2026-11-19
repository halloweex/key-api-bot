#!/usr/bin/env python3
"""Land KeyCRM's discount figures on the orders Postgres held before revision 0038.

Revision 0038 adds the discount columns to `bronze.orders` and
`bronze.order_products`, and chain 3 writes them for every order the sync
lands from then on. Every order landed before holds NULL there. This walks
KeyCRM's order list once, oldest id first, 50 orders a page with their line
items, and hands each page to `pg_orders_write.restore_discounts`, which
writes the discount columns of the orders Postgres holds in the version
KeyCRM served — and nothing else (see "The discount backfill" in
`core/pg_orders_write.py`). It is safe to run beside the web container and
safe to run again: a page whose discount is already landed writes nothing.

PACE: one request every `--interval` seconds at most (1.5 s by default, 40 a
minute), so the minute sync keeps its share of KeyCRM's 60-a-minute quota.
About 1 011 pages on 2026-11-16, so about 25 minutes. Not between 05:00 and
05:30 Kyiv, where the status refresh rewrites the last 30 days' headers.

RESUMING: the last line of the log names the page to start from; pages are
sorted by id, so a new order lands on the last page and never shifts the ones
already walked.

WHAT IT PRINTS: counts and order ids only, never a value — a page's payload
carries names and phone numbers, and nothing of it is logged.

Usage, inside the web container (it has the API key, the DSN and the latch's
directory):

    docker exec keycrm-web python /app/scripts/backfill_discounts.py
    docker exec keycrm-web python /app/scripts/backfill_discounts.py --start-page 412
    docker exec keycrm-web python /app/scripts/backfill_discounts.py --pages 2   # a trial

Afterwards, how many stored orders still hold no discount figures (an order
the walk left `moved` or `absent` is the sync's, and lands on its next write):

    SELECT count(*) FROM bronze.orders WHERE products_total IS NULL;
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

sys.path.insert(0, str(Path(__file__).parent.parent))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("backfill_discounts")

PAGE_SIZE = 50
# Ascending by id: a page's contents never shift under the walk. Line items
# with their offer, which is where a line's product id comes from — without
# it every stored line would read as different and keep its NULLs.
PARAMS = {"limit": PAGE_SIZE, "include": "products.offer", "sort": "id"}
DEFAULT_INTERVAL_S = 1.5   # 40 requests a minute
# How many ids of each kind a progress line names, at most.
SAMPLE = 10


def _sample(ids) -> str:
    ids = list(ids)
    more = f" (+{len(ids) - SAMPLE})" if len(ids) > SAMPLE else ""
    return ",".join(str(i) for i in ids[:SAMPLE]) + more


async def walk(client: Any, *, start_page: int, pages: int | None,
               interval_s: float, restore=None, sleep=asyncio.sleep,
               clock=time.monotonic) -> Dict[str, Any]:
    """Page through KeyCRM's orders from `start_page`, restoring each page.

    Returns the totals and `next_page`, which is where a re-run starts; None
    when the list was walked to its end. A KeyCRM or Postgres error stops the
    walk where it is and is raised, after the totals so far are logged."""
    from core.runtime_modes import configure_modes

    if restore is None:
        from core.pg_orders_write import restore_discounts as restore
    # Before the first write: chain 3's writer takes the latch, which is read
    # from disk once and cached.
    configure_modes()

    totals = {"pages": 0, "orders": 0, "headers_written": 0, "lines_written": 0,
              "already_current": 0, "moved": 0, "absent": 0, "lines_differ": 0,
              "refused": 0}
    page, last_page = start_page, None
    next_due = clock()
    while pages is None or totals["pages"] < pages:
        wait = next_due - clock()
        if wait > 0:
            await sleep(wait)
        next_due = clock() + interval_s
        try:
            body = await client.get_orders(dict(PARAMS, page=page))
            data: List[Dict[str, Any]] = body.get("data") or []
            last_page = body.get("last_page") or last_page
            result = await restore(data) if data else None
        except BaseException:
            logger.error("stopped on page %d; resume with --start-page %d; so far %s",
                         page, page, totals)
            raise
        totals["pages"] += 1
        totals["orders"] += len(data)
        if result is not None:
            totals["headers_written"] += result.headers_written
            totals["lines_written"] += result.lines_written
            for key in ("already_current", "moved", "absent", "lines_differ", "refused"):
                totals[key] += len(getattr(result, key))
            logger.info(
                "page %d/%s: %d orders, %d written (%d with lines), %d current; "
                "moved [%s] absent [%s] lines differ [%s] refused [%s]",
                page, last_page or "?", len(data), result.headers_written,
                result.lines_written, len(result.already_current),
                _sample(result.moved), _sample(result.absent),
                _sample(result.lines_differ), _sample(i for i, _ in result.refused))
        if len(data) < PAGE_SIZE:
            logger.info("walked to the end on page %d: %s", page, totals)
            return dict(totals, next_page=None)
        page += 1
    logger.info("stopped after %d page(s); resume with --start-page %d: %s",
                totals["pages"], page, totals)
    return dict(totals, next_page=page)


async def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n", 1)[0])
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument("--pages", type=int, default=None,
                        help="stop after this many pages (default: to the end)")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_S,
                        help="seconds between requests, at least (default 1.5: 40 a minute)")
    args = parser.parse_args(argv)
    if args.start_page < 1 or (args.pages is not None and args.pages < 1):
        parser.error("--start-page and --pages are at least 1")
    if args.interval < 1.0:
        parser.error("--interval below 1.0 s would take the sync's share of the quota")

    from core.keycrm import KeyCRMClient

    async with KeyCRMClient() as client:
        await walk(client, start_page=args.start_page, pages=args.pages,
                   interval_s=args.interval)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
