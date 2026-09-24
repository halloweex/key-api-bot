"""Parsing `silver.order_utm` inside Postgres, from `bronze.orders` (step 9b).

Until now Postgres has never computed this table. `core/pg_order_utm.py` copies
it out of DuckDB, whose parser runs inside the warehouse tick — so the only
automatic UTM parse in the system lives in the code the DuckDB exit stops, and
a copy of a frozen DuckDB table (or, after a Sunday compaction, an empty one)
would keep shipping under an OK watermark. This module moves the parse to the
rows Postgres already holds: `bronze.orders.manager_comment` is written by the
same mirror as DuckDB's `orders.manager_comment`, from the same parsed payload,
with the same `COALESCE(new, stored)`.

**Not wired.** Nothing in production calls this yet. DN-19 puts it behind
`KS_UTM_PARSE` (default `duckdb`), at the end of the derivation and behind the
four re-parse doors; until then `ship_order_utm` stays the only writer, and the
two must never both write the table in one deploy.

ONE PARSER, NOT TWO

The verdict comes from `core.utm_classify.utm_columns` — the function DuckDB's
parse calls — over the same selection: an order with a non-empty comment and
either no row yet or `updated_at > parsed_at`, stamped with `parsed_at =` the
order's own `updated_at`. `tests/integration/test_pg_utm_parse.py` runs both
engines over the golden fixtures and compares every column at zero tolerance.
A SQL re-derivation of the classifier would be a second one (revision 0018's
argument); running the first one against a second store is not.

TWO SHAPES

- `parse_incremental` — the tick's shape: what is new or changed, upserted.
  **Stamps the watermark on every success, including zero rows**, so
  `last_ok_at` measures that the parser is alive rather than that somebody
  ordered something. `mirror_orders` refuses to move its watermark on nothing
  shipped, and the ~90-minute canary limit would then page every quiet night.
- `parse_full` — the reclassify's shape: every commented order re-parsed, the
  table replaced whole in one transaction, so a reader sees the old verdicts
  or the new ones and never DuckDB's 281-second half-deleted middle. It is the
  only way a rule change reaches orders whose `updated_at` has not moved.

WHAT SERIALISES THEM

`PG_LAYER_LOCK` is per process, and the full parse takes it — bounded, because
three of its future callers are HTTP handlers. A full parse can also be run
from a `docker exec` CLI, which is a process of its own, so both shapes take
`pg_advisory_xact_lock(ADVISORY_LOCK_KEY)` inside their write transaction and
read `bronze.orders` only once they hold it. Two parses therefore never
interleave anywhere, and the second always reads what the first committed.
`parse_incremental` does not take `PG_LAYER_LOCK` itself: its caller, the
derivation, already holds it, and `asyncio.Lock` is not reentrant.

A FULL PARSE REFUSES A SHRINK

Replacing the table whole replaces it with whatever `bronze.orders` holds, and
a `bronze.orders` that was truncated, half-restored or read from the wrong
database holds less than the truth. A missing row here is not a gap anybody
sees — the order falls through the readers' `COALESCE` to organic or
unattributed, on `/traffic` and in the Monday traffic report. So a full parse
yielding under `FULL_PARSE_FLOOR_PCT` of the rows the table holds is refused,
recorded in the watermark and returned, not raised, unless `force=True`.

FAILURES

Any error is written to `meta.mirror_state` through `core.pg_landing`'s
`_record_failure` and then raised — loud, like `ship_order_utm`: the caller
decides what a failure costs, and a parser that reported success while writing
nothing is the worst outcome available. Every write is one transaction, so a
failure leaves the table exactly as it was. No migration: `parsed_at` exists
since revision 0018.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Sequence

from core.pg_order_utm import UTM_COLUMNS, UTM_TABLE
from core.utm_classify import utm_columns

logger = logging.getLogger(__name__)

# `int.from_bytes(b"ks:utm", "big")`. One key for every UTM parse in every
# process, and for nothing else: an advisory lock is a number, and a number
# another module happened to pick would serialise two unrelated jobs.
ADVISORY_LOCK_KEY = 118142646187117

# How long either shape waits for a lock before giving up — `PG_LAYER_LOCK` for
# the full parse, and inside Postgres (`lock_timeout`) the advisory lock and the
# full parse's TRUNCATE, which waits for every `/traffic` read in flight.
# `core/pg_order_utm.py`'s number, for its reason: a normal wait is one Silver
# tick, so this fires only on a genuine hang.
LOCK_WAIT_S = 120

# The share of the table's current rows a full parse must yield to replace it,
# as a whole percentage so the comparison is integer arithmetic and 90 of 100
# is on the line rather than a float's rounding either side of it. Not zero
# tolerance: a comment KeyCRM has since emptied loses its verdict on a full
# parse (the DuckDB reclassify does the same), and that is legitimate. Ten per
# cent is ~3,300 orders at 2026-09 volume — far more than that, and nowhere
# near the whole-table loss an emptied `bronze.orders` makes.
FULL_PARSE_FLOOR_PCT = 90

# `core/pg_operational.py`'s chunk, for its reason.
CHUNK = 5000

# What needs parsing: `TrafficMixin._parse_utm_into_silver`'s predicate over
# DuckDB's `orders`, read against `bronze.orders`. Ordered so a run's writes
# and its log are reproducible.
INCREMENTAL_SELECT_SQL = f"""
SELECT o.id, o.manager_comment, o.updated_at
FROM bronze.orders o
LEFT JOIN {UTM_TABLE} u ON u.order_id = o.id
WHERE o.manager_comment IS NOT NULL
  AND o.manager_comment != ''
  AND (
      u.order_id IS NULL
      OR o.updated_at > u.parsed_at
  )
ORDER BY o.id
"""

# Every order the parse could say anything about. The DuckDB reclassify is a
# DELETE followed by the incremental parse, which after the DELETE selects
# exactly this.
FULL_SELECT_SQL = """
SELECT o.id, o.manager_comment, o.updated_at
FROM bronze.orders o
WHERE o.manager_comment IS NOT NULL
  AND o.manager_comment != ''
ORDER BY o.id
"""

COUNT_SQL = f"SELECT count(*) FROM {UTM_TABLE}"


def _values() -> str:
    # `parsed_at` is the order's `updated_at`, and the wall clock only when the
    # order has none — DuckDB's `COALESCE(?, CURRENT_TIMESTAMP)`, so an order
    # with no `updated_at` still gets a verdict and is not re-parsed forever.
    params = [f"${i}" for i in range(1, len(UTM_COLUMNS))]
    params.append(f"COALESCE(${len(UTM_COLUMNS)}::timestamptz, now())")
    return ", ".join(params)


def insert_sql() -> str:
    return f"INSERT INTO {UTM_TABLE} ({', '.join(UTM_COLUMNS)}) VALUES ({_values()})"


def upsert_sql() -> str:
    # Every column replaced, as DuckDB's `INSERT OR REPLACE` does: a re-parse
    # that now finds nothing in a field must clear it, not keep the old value.
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in UTM_COLUMNS if c != "order_id")
    return (
        f"{insert_sql()}\n"
        f"ON CONFLICT (order_id) DO UPDATE SET {sets}, mirrored_at = now()"
    )


def parse_rows(records: Sequence[Any]) -> List[tuple]:
    """`(id, manager_comment, updated_at)` records → rows in `UTM_COLUMNS` order.

    Pure and synchronous, so it runs in a thread: 0.39 s for 32,656 comments,
    measured, is long enough to matter on the event loop that serves the
    dashboard. The last element is `updated_at` itself, possibly None; the SQL
    turns None into the transaction's clock.
    """
    return [(r[0], *utm_columns(r[1]), r[2]) for r in records]


def refusal(parsed_rows: int, current_rows: int) -> Optional[str]:
    """Why a full parse must not replace the table, or None if it may.

    Pure. The text goes into `meta.mirror_state.last_error`, whose first 300
    characters `mirror_failing` quotes, so both counts and the lever come first.
    """
    # `current_rows > 0` is implied by the inequality and stated anyway: a
    # first parse into an empty table has nothing to protect.
    if current_rows > 0 and parsed_rows * 100 < current_rows * FULL_PARSE_FLOOR_PCT:
        return (
            f"refused: a full UTM parse of bronze.orders yields {parsed_rows} "
            f"rows against the {current_rows} in {UTM_TABLE}, under "
            f"{FULL_PARSE_FLOOR_PCT}%; nothing replaced. → check bronze.orders "
            f"is whole; a shrink that is meant runs with force=True"
        )
    return None


async def _write_chunked(conn, sql: str, rows: Sequence[tuple]) -> None:
    for start in range(0, len(rows), CHUNK):
        await conn.executemany(sql, rows[start:start + CHUNK])


async def _lock(conn) -> None:
    """Bound every lock wait in this transaction, then take the parse's own.

    `SET LOCAL`, so the bound ends with the transaction and never leaks into a
    pooled connection's next user.
    """
    await conn.execute(f"SET LOCAL lock_timeout = '{int(LOCK_WAIT_S)}s'")
    await conn.execute("SELECT pg_advisory_xact_lock($1)", ADVISORY_LOCK_KEY)


async def parse_incremental(pool=None) -> Dict[str, Any]:
    """Parse what is new or changed in `bronze.orders` into the table. Raises.

    One transaction: the advisory lock, the selection, the parse (in a thread),
    the upsert and the OK watermark. Reading after the lock is what makes a
    concurrent full parse harmless — this run sees what that one committed and
    never overwrites it with a verdict read before it.

    The caller holds `PG_LAYER_LOCK`, or needs no ordering against another
    in-process writer of this table; see the module docstring.
    """
    from core.pg import get_pool, require_revision
    from core.pg_landing import _WATERMARK_OK, _record_failure

    started = time.monotonic()
    try:
        # Both inside the try, `ship_order_utm`'s reason: `require_revision`
        # raising is `web` deployed ahead of `migrate`, which is exactly the
        # failure worth naming in the watermark.
        pool = pool or await get_pool()
        await require_revision()

        async with pool.acquire() as conn:
            async with conn.transaction():
                await _lock(conn)
                records = await conn.fetch(INCREMENTAL_SELECT_SQL)
                rows = await asyncio.to_thread(parse_rows, records)
                if rows:
                    await _write_chunked(conn, upsert_sql(), rows)
                # The whole table, not this run's delta: the watermark's
                # `last_rows` reads as "how much should be here" everywhere
                # else, and a quiet run recording zero would read as empty.
                total = await conn.fetchval(COUNT_SQL)
                # Zero parsed is a success like any other. See the docstring.
                await conn.execute(_WATERMARK_OK, UTM_TABLE, total)
    except Exception as exc:
        await _record_failure(UTM_TABLE, f"{type(exc).__name__}: {exc}")
        raise

    result = {
        "parsed": len(rows),
        "rows": total,
        "duration_ms": int((time.monotonic() - started) * 1000),
    }
    logger.info("Order UTM parsed in Postgres (incremental): %s", result)
    return result


async def parse_full(pool=None, *, force: bool = False) -> Dict[str, Any]:
    """Re-parse every commented order and replace the table whole. Raises on a
    fault; refuses a shrink.

    A refusal returns `{"refused": why, "parsed_rows": n, "current_rows": m}`
    rather than raising — `ship_order_utm`'s contract, for its reason: nothing
    is wrong with Postgres, the input is what is wrong, and the table holding
    its previous verdicts is the correct state. It is still recorded in the
    watermark, so `failures_since_ok` says a full parse was turned away.

    Takes `PG_LAYER_LOCK` itself, so it must never be called by code already
    holding it: `asyncio.Lock` is not reentrant, and the call would wait out
    `LOCK_WAIT_S` on itself and then fail.
    """
    from core.pg_landing import _record_failure
    from core.pg_silver import PG_LAYER_LOCK

    try:
        await asyncio.wait_for(PG_LAYER_LOCK.acquire(), timeout=LOCK_WAIT_S)
    except Exception as exc:
        await _record_failure(
            UTM_TABLE,
            f"{type(exc).__name__}: PG_LAYER_LOCK was not free within "
            f"{LOCK_WAIT_S} s; the full UTM parse did not run",
        )
        raise
    try:
        return await _parse_full_locked(pool, force=force)
    finally:
        PG_LAYER_LOCK.release()


async def _parse_full_locked(pool, *, force: bool) -> Dict[str, Any]:
    from core.pg import get_pool, require_revision
    from core.pg_landing import _WATERMARK_OK, _record_failure

    started = time.monotonic()
    refused: Optional[str] = None
    try:
        pool = pool or await get_pool()
        await require_revision()

        async with pool.acquire() as conn:
            async with conn.transaction():
                await _lock(conn)
                # Counted in the transaction that would truncate, after the
                # lock, so the number compared is the table this replaces.
                current = await conn.fetchval(COUNT_SQL)
                records = await conn.fetch(FULL_SELECT_SQL)
                rows = await asyncio.to_thread(parse_rows, records)
                if not force:
                    refused = refusal(len(rows), current)
                if refused is None:
                    await conn.execute(f"TRUNCATE {UTM_TABLE}")
                    await _write_chunked(conn, insert_sql(), rows)
                    await conn.execute(_WATERMARK_OK, UTM_TABLE, len(rows))
    except Exception as exc:
        await _record_failure(UTM_TABLE, f"{type(exc).__name__}: {exc}")
        raise

    if refused is not None:
        # After the transaction has closed: the recorder takes a connection of
        # its own, and one asked for while this still held one would wait on
        # itself in a pool of size one.
        await _record_failure(UTM_TABLE, refused)
        logger.error("Order UTM full parse refused; the table keeps its rows: %s", refused)
        return {"refused": refused, "parsed_rows": len(rows), "current_rows": current}

    result = {
        "rows": len(rows),
        "replaced": current,
        "duration_ms": int((time.monotonic() - started) * 1000),
    }
    if force:
        result["forced"] = True
    logger.info("Order UTM parsed in Postgres (full): %s", result)
    return result
