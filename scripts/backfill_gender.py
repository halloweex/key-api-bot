#!/usr/bin/env python3
"""Preview or run the gender derivation by hand.

    python scripts/backfill_gender.py --dry-run            # Postgres, read-only
    python scripts/backfill_gender.py --dry-run --duckdb   # the local DuckDB file
    python scripts/backfill_gender.py [--all]              # write — see below

A thin CLI over `core.gender_backfill`, which is also what the hourly scheduler
tick calls, so running this by hand and waiting for the tick produce the same
table.

YOU RARELY NEED THE WRITE PATH, AND IN PRODUCTION YOU CANNOT USE IT

The web container holds the DuckDB file exclusively, and a second process cannot
open it at all — not even read-only. This script's first version advertised
`docker exec keycrm-web python /app/scripts/backfill_gender.py` and that line
could never have worked: it died with an IOException for eleven days before
anyone ran it. The derivation does not need it anyway. It already runs inside
web, on every hourly `replicate_operational` tick and about 90 s after every
start, for each buyer with no verdict or with one from an older RULES_VERSION.
So:

* **a rules change** reaches the table by bumping `core.gender.RULES_VERSION`
  and deploying — the first tick after the start rewrites every stale verdict;
* **"now" rather than within the hour** is
  `POST /api/jobs/replicate_operational/trigger` as an admin, from the owner's
  own terminal — a dashboard session is an unrevocable seven-day admin token and
  must never be pasted anywhere else;
* **the write path here** is for a laptop copy, or for a web container that is
  deliberately stopped. When the file is held by someone else it exits 2 with
  that explanation instead of a stack trace, and writes nothing.

THE PREVIEW IS THE USEFUL HALF

`--dry-run` reads Postgres inside a read-only transaction, so it runs beside a
live web, and answers the question worth asking before a rules change: how many
stored verdicts the current code would decide differently, as `f→m` / `m→NULL`
transitions, and what the split would be afterwards. Counts only — nothing it
prints names a customer. It uses `KS_PG_DSN`; in production that is the web
container's writer DSN, which is exactly why the transaction is read-only.

UNDER CHAIN 4 THE WRITE PATH IS POSTGRES, AND ONLY ONCE WEB HAS WRITTEN THERE

With `KS_WRITE_BUYERS` moving the buyers (`core/pg_buyers_write.py`), the
verdicts are written in Postgres and DuckDB's copy has stopped, so the write
path goes to `derive_gender_pg` — beside a live web, since Postgres takes two
writers and the derivation share-locks what it decides. It goes there only when
the chain is already latched. The latch is a marker file read once per process
and cached (`core/chain_latch.py`): a script that took the first write would
latch a chain web still believes is on DuckDB, and web would go on writing
DuckDB until it restarted. So a chain flagged and not yet latched is refused
(exit 4): let web make the first write — its hourly tick, or the trigger above.
A `KS_WRITE_BUYERS` nobody can read is refused the same way, rather than
guessing which store it meant.

`--dry-run --duckdb` reads a DuckDB that has stopped receiving buyers once the
chain has moved them; the Postgres preview is the one to read then.

WHAT IT WILL NOT DO

It never touches a row with `override_by_human = TRUE`, under any flag. That is
the `managers.is_retail` lesson: the column was recomputed on every sync until a
human's classification became impossible to keep, and eight managers sold ₴3.1M
into the wrong bucket before anyone noticed.

Exit codes: 0 done, 1 the derivation failed, 2 the DuckDB file is held by
another process and nothing was written, 3 no store to read from, 4 chain 4's
state forbids this write (flagged and not latched, or a flag nobody can read)
and nothing was written.
"""
import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import duckdb  # noqa: E402

from core import chain_latch, pg_buyers_write  # noqa: E402
from core.gender_backfill import (  # noqa: E402
    derive_gender, read_stored_duckdb, read_stored_postgres, summarise,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

HELD_ELSEWHERE = """\
The DuckDB file is held by another process — in production, the web container.
Nothing was written.

The derivation already runs inside web: on every hourly replicate_operational
tick and ~90 s after each start, for every buyer with no verdict or with one
from an older RULES_VERSION.
  - to rewrite verdicts after a rules change: bump core.gender.RULES_VERSION
    and deploy;
  - to run it now: POST /api/jobs/replicate_operational/trigger as an admin,
    from your own terminal;
  - to see what would change first: rerun with --dry-run (reads Postgres,
    read-only)."""

EXIT_OK, EXIT_FAILED, EXIT_HELD, EXIT_NO_STORE, EXIT_CHAIN = 0, 1, 2, 3, 4

NOT_LATCHED = """KS_WRITE_BUYERS says postgres, but chain 4 has not written Postgres yet.
Nothing was written.

Its first write latches the chain, and the latch is a marker web reads once
and caches: taken here, it would move a chain web still believes is on DuckDB,
and web would keep writing DuckDB until it restarted. Let web make the first
write — the hourly replicate_operational tick, or
POST /api/jobs/replicate_operational/trigger as an admin — then rerun."""


def held_by_another_process(exc: BaseException) -> bool:
    """True for DuckDB's refusal to open a file another process holds.

    Matched on the lock, not on IOException alone: a missing directory or a
    full disk is also an IOException, and saying "web is running" about either
    would send the reader to the wrong place.
    """
    return isinstance(exc, duckdb.IOException) and "lock" in str(exc).lower()


async def open_store():
    """The store, or None when another process holds the file."""
    from core.duckdb_store import get_store

    try:
        return await get_store()
    except duckdb.IOException as exc:
        if held_by_another_process(exc):
            return None
        raise


async def preview(use_duckdb: bool) -> int:
    if use_duckdb:
        store = await open_store()
        if store is None:
            logger.error("%s", HELD_ELSEWHERE.splitlines()[0])
            logger.error("Use --dry-run without --duckdb: it reads Postgres.")
            return EXIT_HELD
        rows = await read_stored_duckdb(store)
        source = "DuckDB"
    else:
        from core import pg

        try:
            dsn = pg.dsn()
        except Exception as exc:  # noqa: BLE001 — reported, not swallowed
            logger.error("No Postgres to read from (%s). "
                         "For a local copy use --dry-run --duckdb.", exc)
            return EXIT_NO_STORE
        import asyncpg

        conn = await asyncpg.connect(dsn)
        try:
            rows = await read_stored_postgres(conn)
        finally:
            await conn.close()
        source = "Postgres"

    summary = summarise(rows)
    logger.info("preview from %s, rules v%d — nothing written",
                source, summary["rules_version"])
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return EXIT_OK


async def run(rebuild_all: bool) -> int:
    from core.runtime_modes import configure_modes

    # The latch markers are loaded here; without this the chain's answer below
    # would be the flag's alone.
    configure_modes()
    mode = pg_buyers_write.mode()
    if mode is None:
        logger.error("KS_WRITE_BUYERS is not understood; nothing was written. "
                     "Correct it, or remove it for DuckDB.")
        return EXIT_CHAIN
    if mode == "postgres":
        if not chain_latch.latched(pg_buyers_write.CHAIN):
            print(NOT_LATCHED, file=sys.stderr)
            return EXIT_CHAIN
        result = await pg_buyers_write.derive_gender_pg(rebuild_all=rebuild_all)
        where = "Postgres, where chain 4 writes the verdicts"
    else:
        store = await open_store()
        if store is None:
            print(HELD_ELSEWHERE, file=sys.stderr)
            return EXIT_HELD
        result = await derive_gender(store, rebuild_all=rebuild_all)
        where = "DuckDB only — Postgres receives it on the next hourly tick"

    if result["error"]:
        logger.error("failed: %s", result["error"])
        return EXIT_FAILED
    logger.info(
        "done: %d of %d written in %d ms (female %d, male %d, NULL %d). %s.",
        result["written"], result["pending"], result["ms"],
        result["female"], result["male"], result["null"], where,
    )
    return EXIT_OK


async def main(rebuild_all: bool, dry_run: bool, use_duckdb: bool) -> int:
    if dry_run:
        return await preview(use_duckdb)
    return await run(rebuild_all)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true",
                    help="preview what the rules would change; writes nothing")
    ap.add_argument("--duckdb", action="store_true",
                    help="with --dry-run: read the local DuckDB file, not Postgres")
    ap.add_argument("--all", action="store_true",
                    help="write path: re-derive every buyer except human overrides")
    args = ap.parse_args()
    if args.duckdb and not args.dry_run:
        ap.error("--duckdb only applies to --dry-run")
    sys.exit(asyncio.run(main(args.all, args.dry_run, args.duckdb)))
