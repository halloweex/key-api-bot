#!/usr/bin/env python3
"""Preview or run the gender derivation by hand.

    python scripts/backfill_gender.py --dry-run            # Postgres, read-only
    python scripts/backfill_gender.py [--all]              # write — see below

A thin CLI over chain 4's derivation (`core.pg_buyers_write.derive_gender_pg`),
which is also what the hourly scheduler tick calls, so running this by hand and
waiting for the tick produce the same table.

YOU RARELY NEED THE WRITE PATH

The derivation already runs inside web, on every hourly `replicate_operational`
tick and about 90 s after every start, for each buyer with no verdict or with
one from an older RULES_VERSION. So:

* **a rules change** reaches the table by bumping `core.gender.RULES_VERSION`
  and deploying — the first tick after the start rewrites every stale verdict;
* **"now" rather than within the hour** is
  `POST /api/jobs/replicate_operational/trigger` as an admin, from the owner's
  own terminal — a dashboard session is an unrevocable seven-day admin token and
  must never be pasted anywhere else;
* **the write path here** writes Postgres beside a live web: Postgres takes two
  writers, and the derivation share-locks what it decides.

THE PREVIEW IS THE USEFUL HALF

`--dry-run` reads Postgres inside a read-only transaction, so it runs beside a
live web, and answers the question worth asking before a rules change: how many
stored verdicts the current code would decide differently, as `f→m` / `m→NULL`
transitions, and what the split would be afterwards. Counts only — nothing it
prints names a customer. It uses `KS_PG_DSN`; in production that is the web
container's writer DSN, which is exactly why the transaction is read-only.

UNTIL STEP 14'S S14-11

The write path followed chain 4's `mode()`: DuckDB's `derive_gender` while the
chain wrote DuckDB, Postgres only once web had latched it (exit 4 otherwise, so
that a script never took the first write of a chain web believed was on
DuckDB), and `--dry-run --duckdb` read the local DuckDB file. The chain writes
Postgres, a constant since S14-00, and S14-11 deleted the DuckDB half with the
flag: this script never imports `duckdb`.

WHAT IT WILL NOT DO

It never touches a row with `override_by_human = TRUE`. That is the
`managers.is_retail` lesson: the column was recomputed on every sync until a
human's classification became impossible to keep, and eight managers sold
₴3.1M into the wrong bucket before anyone noticed.

Exit codes: 0 done, 1 the derivation failed, 3 no Postgres to read from.
"""
import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from core import pg_buyers_write  # noqa: E402
from core.gender_backfill import read_stored_postgres, summarise  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

EXIT_OK, EXIT_FAILED, EXIT_NO_STORE = 0, 1, 3


async def preview() -> int:
    from core import pg

    try:
        dsn = pg.dsn()
    except Exception as exc:  # noqa: BLE001 — reported, not swallowed
        logger.error("No Postgres to read from (%s).", exc)
        return EXIT_NO_STORE
    import asyncpg

    conn = await asyncpg.connect(dsn)
    try:
        rows = await read_stored_postgres(conn)
    finally:
        await conn.close()

    summary = summarise(rows)
    logger.info("preview from Postgres, rules v%d — nothing written",
                summary["rules_version"])
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return EXIT_OK


async def run(rebuild_all: bool) -> int:
    from core.runtime_modes import configure_modes

    # Before the first write, as every script that writes does
    # (`tests/unit/test_runtime_modes.py`): the modes a write reads are
    # configured here, the latch markers among them.
    configure_modes()
    result = await pg_buyers_write.derive_gender_pg(rebuild_all=rebuild_all)
    if result["error"]:
        logger.error("failed: %s", result["error"])
        return EXIT_FAILED
    logger.info(
        "done: %d of %d written in %d ms (female %d, male %d, NULL %d). "
        "Postgres, where chain 4 writes the verdicts.",
        result["written"], result["pending"], result["ms"],
        result["female"], result["male"], result["null"],
    )
    return EXIT_OK


async def main(rebuild_all: bool, dry_run: bool) -> int:
    if dry_run:
        return await preview()
    return await run(rebuild_all)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true",
                    help="preview what the rules would change; writes nothing")
    ap.add_argument("--all", action="store_true",
                    help="write path: re-derive every buyer except human overrides")
    args = ap.parse_args()
    sys.exit(asyncio.run(main(args.all, args.dry_run)))
