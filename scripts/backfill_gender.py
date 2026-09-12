#!/usr/bin/env python3
"""Derive a gender for every buyer and write it to `buyer_gender`.

    python scripts/backfill_gender.py --dry-run     # classify, write nothing
    python scripts/backfill_gender.py               # new + stale rows only
    python scripts/backfill_gender.py --all         # re-derive every row
    docker exec keycrm-web python /app/scripts/backfill_gender.py

WHAT IT WILL NOT DO

  * It never touches a row with `override_by_human = TRUE`. That is the whole
    reason the column exists: `managers.is_retail` was recomputed on every sync
    until a human's classification became impossible to keep, and the fix cost
    ₴3.1M of misclassified revenue to notice. Not repeated here.
  * It never deletes a buyer's row to "clean up". A verdict that becomes
    undecidable is written as NULL, which is a decision, not an absence.

WHY IT IS INCREMENTAL BY DEFAULT

The classifier is deterministic, so re-deriving an unchanged name produces an
identical row and the write is pure cost. Steady state on this base is ~19 new
buyers a day. A full pass is only needed when `RULES_VERSION` moves, and that is
exactly what the default mode detects — rows stamped with an older version are
re-derived without anyone having to remember to pass `--all`.

WHY IT WRITES IN BATCHES

DuckDB holds a process-wide store lock, and the warehouse rebuild takes it every
two minutes. Holding it for one 20 000-row transaction is how the SMS tab went
from 124 to 38 req/s once already. The work is chunked and the lock released
between chunks; a run interrupted halfway leaves a consistent table and the next
run finishes it, because "what still needs doing" is recomputed rather than
stored in a cursor.
"""
import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core.duckdb_store import get_store
from core.gender import RULES_VERSION, classify

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

CHUNK = 2000


async def _pending(store, rebuild_all: bool):
    """Buyers needing a verdict: new, stale, or everything but the overrides."""
    if rebuild_all:
        sql = """
            SELECT b.id, b.full_name
            FROM buyers b
            LEFT JOIN buyer_gender g ON g.buyer_id = b.id
            WHERE g.buyer_id IS NULL OR g.override_by_human = FALSE
            ORDER BY b.id
        """
    else:
        sql = f"""
            SELECT b.id, b.full_name
            FROM buyers b
            LEFT JOIN buyer_gender g ON g.buyer_id = b.id
            WHERE g.buyer_id IS NULL
               OR (g.override_by_human = FALSE AND g.rules_version < {RULES_VERSION})
            ORDER BY b.id
        """
    async with store.connection() as conn:
        return conn.execute(sql).fetchall()


async def _write(store, verdicts) -> int:
    """Replace the verdicts for these buyers. DELETE then INSERT, never upsert.

    The table has no primary key — an ART index disables vacuum for the whole
    table in DuckDB — so uniqueness is the writer's job, and deleting the ids
    about to be written is how it is kept.
    """
    if not verdicts:
        return 0
    ids = [v[0] for v in verdicts]
    rows = [
        (bid, v.gender, v.method, v.confidence, v.decided_from, RULES_VERSION)
        for bid, v in verdicts
    ]
    async with store.connection() as conn:
        conn.execute("BEGIN TRANSACTION")
        try:
            placeholders = ",".join("?" * len(ids))
            conn.execute(
                f"DELETE FROM buyer_gender WHERE buyer_id IN ({placeholders})", ids
            )
            conn.executemany(
                """INSERT INTO buyer_gender
                   (buyer_id, gender, method, confidence, decided_from, rules_version)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                rows,
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    return len(rows)


async def main(rebuild_all: bool, dry_run: bool) -> int:
    store = await get_store()

    pending = await _pending(store, rebuild_all)
    logger.info(
        "buyers to classify: %d (rules_version=%d, mode=%s)",
        len(pending), RULES_VERSION, "all" if rebuild_all else "incremental",
    )
    if not pending:
        logger.info("nothing to do")
        return 0

    verdicts = [(bid, classify(name)) for bid, name in pending]

    decided = sum(1 for _, v in verdicts if v.gender)
    female = sum(1 for _, v in verdicts if v.gender == "f")
    male = sum(1 for _, v in verdicts if v.gender == "m")
    null = len(verdicts) - decided
    logger.info(
        "classified: %d decided (%.2f%%), female %d, male %d, NULL %d",
        decided, 100.0 * decided / len(verdicts), female, male, null,
    )

    if dry_run:
        logger.info("--dry-run: nothing written")
        return 0

    written = 0
    for i in range(0, len(verdicts), CHUNK):
        written += await _write(store, verdicts[i:i + CHUNK])
        logger.info("written %d/%d", written, len(verdicts))
        # Yield between chunks so the warehouse tick can take the store lock.
        await asyncio.sleep(0)

    logger.info("done: %d rows written", written)
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--all", action="store_true",
                    help="re-derive every buyer except human overrides")
    ap.add_argument("--dry-run", action="store_true",
                    help="classify and report, write nothing")
    args = ap.parse_args()
    sys.exit(asyncio.run(main(args.all, args.dry_run)))
