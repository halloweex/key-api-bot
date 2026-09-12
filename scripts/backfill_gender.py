#!/usr/bin/env python3
"""Derive a gender for every buyer and write it to `buyer_gender`.

    python scripts/backfill_gender.py --dry-run     # classify, write nothing
    python scripts/backfill_gender.py               # new + stale rows only
    python scripts/backfill_gender.py --all         # re-derive every row
    docker exec keycrm-web python /app/scripts/backfill_gender.py

A thin CLI over `core.gender_backfill.derive_gender`, which is also what the
hourly scheduler tick calls. The logic lives there and not here so the two
callers cannot drift — running this by hand and waiting for the tick must
produce the same table.

You rarely need this. The scheduler derives new buyers every hour and re-derives
everything when `core.gender.RULES_VERSION` moves, so the ordinary reasons to
run it by hand are: proving a rules change before waiting an hour, and `--all`
after editing the name tables.

WHAT IT WILL NOT DO

It never touches a row with `override_by_human = TRUE`, under any flag. That is
the `managers.is_retail` lesson: the column was recomputed on every sync until a
human's classification became impossible to keep, and eight managers sold ₴3.1M
into the wrong bucket before anyone noticed.
"""
import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.duckdb_store import get_store
from core.gender import RULES_VERSION, classify
from core.gender_backfill import derive_gender, pending

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def main(rebuild_all: bool, dry_run: bool) -> int:
    store = await get_store()

    if dry_run:
        rows = await pending(store, rebuild_all=rebuild_all)
        logger.info("buyers to classify: %d (rules v%d)", len(rows), RULES_VERSION)
        if not rows:
            return 0
        verdicts = [classify(name) for _, name in rows]
        female = sum(1 for v in verdicts if v.gender == "f")
        male = sum(1 for v in verdicts if v.gender == "m")
        null = len(verdicts) - female - male
        logger.info(
            "would write: female %d, male %d, NULL %d (%.2f%% decided)",
            female, male, null, 100.0 * (female + male) / len(verdicts),
        )
        logger.info("--dry-run: nothing written")
        return 0

    result = await derive_gender(store, rebuild_all=rebuild_all)
    if result["error"]:
        logger.error("failed: %s", result["error"])
        return 1
    logger.info(
        "done: %d of %d written in %d ms (female %d, male %d, NULL %d)",
        result["written"], result["pending"], result["ms"],
        result["female"], result["male"], result["null"],
    )
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--all", action="store_true",
                    help="re-derive every buyer except human overrides")
    ap.add_argument("--dry-run", action="store_true",
                    help="classify and report, write nothing")
    args = ap.parse_args()
    sys.exit(asyncio.run(main(args.all, args.dry_run)))
