"""Shipping `silver.order_utm` from DuckDB, on the Silver tick.

The one Silver object this store cannot compute for itself. `silver.orders` is
projected from `bronze.orders` and `silver.order_lines` is a view whose body is
the same text DuckDB uses, but the UTM level is parsed out of `manager_comment`
in Python — regexes over a free-text field, then `_classify_traffic` deciding
between `paid_confirmed`, `paid_likely`, `organic`, `pixel_only` and `unknown`.
A SQL derivation would be a second implementation of that classifier and would
drift from the first the day either was touched, so the verdict is copied and
never recomputed. Revision 0018 carries the argument; `app.manager_classifications`
made the same call one layer down.

WHY IT RIDES THE SILVER TICK AND NOT `replicate_operational`

Because a stale row here does not read as missing data — it reads as a
different answer. An order whose UTM has not arrived yet falls through the
`COALESCE` to `organic` or `unknown`, so the paid/organic split on the chart
moves, and it moves in one store and not the other. The hourly family can
afford its lag because a missing `stock_movement` looks like nothing at all.
This cannot, so Silver and its classification land in the same tick, under the
same lock, which is what DuckDB already does one store over.

FULL REPLACE, AND WHY NOT A WATERMARK

`parsed_at` would make a cheap delta and would be wrong twice. The admin
reparse endpoint and `scripts/backfill_utm.py` both `DELETE FROM
silver_order_utm` and rebuild it, and a run that produced *fewer* rows is
invisible to any `>` filter — `stock_movements`' hole, and here there is no
daily comparison ahead of the read to catch it. The table is 32,905 rows
against the 46,446 `silver.orders` already replaced whole in this same
transaction, so the delta would save a third of a cost that is already paid.

A FULL REPLACE COPIES A SHRINK, SO IT REFUSES ONE

The same argument cuts the other way. Whatever DuckDB holds at the moment of
the read is what Postgres holds a transaction later, and three things leave
DuckDB holding less than the truth: a reclassify whose DELETE has committed
and whose re-parse is still running (281 s on 2026-09-09) or raised, the same
shape in `scripts/backfill_utm.py`, and a warehouse tick whose parse raised
between two write batches — which the tick logs as a warning and then ships
anyway. `/traffic` and the weekly traffic report read Postgres, and a missing
row there is not a gap anybody sees: it falls through the COALESCE and the
order is filed as unattributed or organic, on screen and in the Monday
message, with nothing to say so.

So `ship_order_utm` compares before it truncates, inside the transaction that
would truncate, and refuses a copy under `SHRINK_FLOOR_PCT` of what Postgres
holds, or any copy while the last parse is recorded as failed. The table never
legitimately shrinks: nothing deletes from it but the two full re-parses, and
they put every row back before they ship. A refusal is not an exception —
the scheduler would read one as a failed layer — but it is written into the
watermark, so the daily comparison names it rather than reporting the rows
Postgres kept as a difference. `force=True` overrides both, and is passed from
nowhere but the CLI.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

UTM_TABLE = "silver.order_utm"

# The write order, and the read order. Postgres holds one column DuckDB does
# not (`mirrored_at`, bookkeeping) and no column DuckDB lacks, so this tuple is
# both the projection and the comparison's column list.
UTM_COLUMNS: Tuple[str, ...] = (
    "order_id",
    "utm_source", "utm_medium", "utm_campaign", "utm_content",
    "utm_term", "utm_lang",
    "fbp", "fbc", "ttp", "fbclid",
    "traffic_type", "platform",
    "parsed_at",
)

# `core/pg_operational.py`'s number, for its reason: one `executemany` carrying
# every row is a single round trip holding one very large parameter list.
CHUNK = 5000

# How long a reparse will wait for `PG_LAYER_LOCK` before giving up. Generous
# against a normal wait (~3 s, the length of one Silver tick) and short enough
# that an admin request cannot be parked indefinitely behind a stuck shipper.
LOCK_WAIT_S = 120

# The share of Postgres' rows a DuckDB copy must still hold to be shipped, as a
# whole percentage so the comparison is integer arithmetic and 900 of 1,000 is
# exactly on the line rather than a float's rounding either side of it.
#
# Not zero tolerance, although the table never legitimately shrinks: a copy
# read while a reclassify is in its last stretch is short only by what that
# reclassify has still to write, and its own ship overwrites the copy moments
# later. A guard that refused it would put a failure in the watermark for a
# state that heals itself. Ten per cent is ~3,300 orders at 2026-09 volume
# (33,203 rows): wide enough for that stretch, and nowhere near the
# whole-table loss a reclassify's DELETE makes, which is what this exists for.
SHRINK_FLOOR_PCT = 90


def refusal(duckdb_rows: int, postgres_rows: int,
            parse_error: Optional[str]) -> Optional[str]:
    """Why this copy must not replace Postgres' table, or None if it may.

    Pure, so the rule can be read and tested without either database. The text
    goes into `meta.mirror_state.last_error`, whose first 300 characters the
    daily `mirror_failing` finding quotes — so both counts come first and the
    lever comes before the explanation runs out of room.
    """
    if parse_error is not None:
        return (
            f"refused: the last UTM parse failed ({parse_error[:120]}), so "
            f"DuckDB's {duckdb_rows} rows may be half-written; Postgres keeps "
            f"its {postgres_rows}. → the next dirty warehouse tick re-parses"
        )
    # `postgres_rows > 0` is implied by the inequality — no count is below
    # zero — and stated anyway: a first ship into an empty table has nothing
    # to protect, and that should not depend on reading the arithmetic.
    if postgres_rows > 0 and duckdb_rows * 100 < postgres_rows * SHRINK_FLOOR_PCT:
        return (
            f"refused: DuckDB holds {duckdb_rows} UTM rows against Postgres' "
            f"{postgres_rows}, under {SHRINK_FLOOR_PCT}%, most likely a "
            f"re-parse still running; not shipped. → let the re-parse finish; "
            f"a shrink that is meant ships only with force=True"
        )
    return None


def _parse_error(store) -> Optional[str]:
    """The store's record of its last UTM parse, or None.

    `isinstance`, not truthiness: the unit tests hand this module an
    `AsyncMock` for a store, whose every attribute is a truthy mock, and a
    guard that read one of those as a failed parse would refuse every ship in
    the suite for a reason that exists nowhere in production.
    """
    error = getattr(store, "last_utm_parse_error", None)
    return error if isinstance(error, str) else None


def _insert() -> str:
    values = ", ".join(f"${i}" for i in range(1, len(UTM_COLUMNS) + 1))
    return f"INSERT INTO {UTM_TABLE} ({', '.join(UTM_COLUMNS)}) VALUES ({values})"


def read_utm(conn) -> List[tuple]:
    """Every UTM row out of DuckDB, in the column order Postgres wants.

    Ordered by the key because the rows go into a table that was truncated a
    moment earlier, so its primary-key index is built from scratch and appends
    in key order rather than splitting pages at random. Not for recovery: the
    write is one transaction, so there is no partial state for a successor run
    to line up against.
    """
    rows = conn.execute(
        f"SELECT {', '.join(UTM_COLUMNS)} FROM silver_order_utm ORDER BY order_id"
    ).fetchall()
    return [tuple(r) for r in rows]


async def ship_order_utm(store, pool=None, *, force: bool = False) -> Dict[str, Any]:
    """Replace `silver.order_utm` from DuckDB. Raises on a fault; refuses a
    shrink.

    Loud, like `rebuild_silver` beside which it runs and unlike the mirror in
    the sync path: the caller treats a failure of any Postgres layer as a
    reason to leave the whole tick unstamped, and a shipper that reported
    success while writing nothing is the worst available outcome.

    **A refusal returns rather than raises**, as `{"refused": why,
    "duckdb_rows": n, "postgres_rows": m}`. Nothing is wrong with Postgres
    or the connection — the copy is what is wrong, and Postgres holding its
    previous one is the correct state. Raising would make
    `_rebuild_postgres_layers` log a failed layer and keep the rebuild
    pending, retrying the same refusal every tick past the floor, and make
    `ship_after_reparse` report a Postgres fault. The refusal is still loud
    where it counts: an ERROR in the log and a failure in the watermark.

    The DuckDB read and the Postgres round trips are in separate blocks. The
    store's lock is not reentrant and every other reader waits behind it, so
    awaiting the network while holding it would stall the dashboard for the
    length of the copy — `replicate_operational`'s rule, and the reason this
    is called from outside every `store.connection()` block.
    """
    from core.pg import get_pool, require_revision
    from core.pg_landing import _WATERMARK_OK, _record_failure

    started = time.monotonic()
    refused: Optional[str] = None
    postgres_rows = 0

    try:
        # Inside the try, both of them. `require_revision` raising is the
        # deploy-order fault — `web` ahead of `migrate` — and that is exactly
        # the failure worth naming in the watermark rather than discovering
        # the next morning as row differences. `get_pool` raising usually
        # cannot be recorded at all (the recorder needs the same pool, and
        # says so), but it costs nothing to try and the contract is then
        # simply "every failure is reported or attempted".
        pool = pool or await get_pool()
        await require_revision()

        # Timed separately because this is the part that costs the *whole
        # dashboard*: the store lock is global, so every other reader waits
        # behind this read. The Postgres half costs only this table.
        read_started = time.monotonic()
        async with store.connection() as conn:
            rows = read_utm(conn)
        read_ms = int((time.monotonic() - read_started) * 1000)
        # After the read, not before it. The parser records its outcome only
        # once it has let go of the store, so a parse that raised while this
        # read waited for the lock — its earlier batches already in what was
        # just read — is visible here and would not have been a line earlier.
        parse_error = _parse_error(store)

        async with pool.acquire() as conn:
            async with conn.transaction():
                # Counted in the transaction that would truncate, so the number
                # compared is the table this write replaces. The two shippers
                # in the web process cannot interleave here — both hold
                # `PG_LAYER_LOCK` — so no table lock is taken before the count:
                # an ACCESS EXCLUSIVE held across a refusal would stall
                # `/traffic` to write nothing. (`scripts/backfill_utm.py` runs
                # in a process of its own, so its lock is its own; that is an
                # operator's hand-run, and was true before this guard.)
                postgres_rows = await conn.fetchval(f"SELECT count(*) FROM {UTM_TABLE}")
                if not force:
                    refused = refusal(len(rows), postgres_rows, parse_error)
                if refused is None:
                    await conn.execute(f"TRUNCATE {UTM_TABLE}")
                    await _write_chunked(conn, _insert(), rows)
                    # The same watermark table the mirror and the Silver
                    # rebuild use, so the comparison can tell "not shipped
                    # yet" from "shipped wrong" without a second mechanism to
                    # learn.
                    await conn.execute(_WATERMARK_OK, UTM_TABLE, len(rows))
    except Exception as exc:
        # Say so in the watermark before raising — `core/pg_landing.py`'s
        # `_record_failure`, borrowed rather than rewritten.
        #
        # Without it a shipper that keeps failing leaves `last_ok_at` at the
        # last success and `failures_since_ok` at zero, so the daily check
        # reads the watermark as healthy, compares the rows anyway, and
        # reports *thousands of row differences* — which names the symptom
        # and hides the cause. With it the finding is `mirror_failing`,
        # carrying the count and the error text. The alerting charter's rule:
        # a CRITICAL names its lever.
        #
        # The derived layers beside this one (`rebuild_silver`, `rebuild_gold`)
        # deliberately do not do this, and they are right not to: they can be
        # recomputed from what Postgres already holds, so a failure costs
        # freshness and nothing else. This is a copy of state only DuckDB has,
        # which puts it with the mirrors.
        await _record_failure(UTM_TABLE, f"{type(exc).__name__}: {exc}")
        raise

    if refused is not None:
        # Recorded after the transaction has closed, not inside it: the
        # recorder takes a connection of its own from the same pool, and one
        # asked for while this function still held one waits on itself in any
        # pool of size one. Nothing was written, so there is nothing to order
        # it against.
        #
        # Into the watermark for `_record_failure`'s reason one level up: a
        # refusal that left `failures_since_ok` at zero would let the daily
        # comparison read a healthy watermark and report the rows Postgres
        # kept as thousands of differences. With it the finding is
        # `mirror_failing`, quoting the counts.
        await _record_failure(UTM_TABLE, refused)
        logger.error(
            "Order UTM not shipped to Postgres, which keeps its previous copy: %s",
            refused,
        )
        return {
            "refused": refused,
            "duckdb_rows": len(rows),
            "postgres_rows": postgres_rows,
        }

    result = {
        "rows": len(rows),
        "read_ms": read_ms,
        "duration_ms": int((time.monotonic() - started) * 1000),
    }
    if force:
        result["forced"] = True
    logger.info("Order UTM shipped to Postgres: %s", result)
    return result


async def _write_chunked(conn, sql: str, rows: Sequence[tuple]) -> None:
    for start in range(0, len(rows), CHUNK):
        await conn.executemany(sql, rows[start:start + CHUNK])


async def ship_after_reparse(store, *, force: bool = False) -> Dict[str, Any]:
    """Ship the UTM straight after a reparse that went round the tick.

    Three admin paths rewrite `silver_order_utm` in DuckDB without marking the
    warehouse dirty — the traffic refresh, the reclassify, and the
    `manager_comment` backfill. Before `/traffic` read Postgres that was
    harmless, because the tab read the table those endpoints had just written.
    It is not harmless now: the tab would keep showing the previous
    classification until the next dirty warehouse tick, and an admin who has
    just changed the rules and is looking at the page would read that as the
    reclassify having failed.

    **Under `PG_LAYER_LOCK`, which is the whole reason this is a function and
    not a line at each call site.** The scheduler ships this table too, and two
    TRUNCATE+INSERT transactions that interleave leave Postgres holding
    whichever copy committed last under a fresh OK watermark — including a copy
    read *before* the reparse. That is the exact failure the lock was
    introduced for.

    **Never raises.** The DuckDB work has already succeeded and the endpoint
    should report it; a Postgres fault costs freshness until the next tick,
    which is where this table was before this function existed. Rule 8 — the
    step leaves the system working.

    **The admin endpoints never pass `force`, and do not need to.** The
    shrink guard reads the table as it stands when this runs, which is after
    the caller's re-parse has returned. The traffic refresh and the
    `manager_comment` backfill never delete, so their parse only adds rows.
    The reclassify DELETEs and then parses every order with a comment and no
    row — which, straight after the DELETE, is every order the table held,
    less any whose comment has since been emptied — so by the time it ships
    DuckDB holds what Postgres does to well within the floor. A successful
    parse has also just cleared `last_utm_parse_error`. The guard can stop
    these callers only when their own parse was cut short, and that is exactly
    the copy it exists to stop. `force` is here for `scripts/backfill_utm.py`,
    behind a flag an operator has to type; a test pins that nothing under
    `core/`, `web/` or `bot/` originates it.
    """
    from core.mirror_reconciliation import configured

    if not configured():
        return {"skipped": "KS_PG_DSN is not set"}
    try:
        from core.pg_silver import PG_LAYER_LOCK

        # Bounded, because three of the four callers are HTTP handlers and
        # `asyncio.Lock` has no timeout of its own. The lock is also held by
        # the ClickHouse shippers across network I/O, so a hung ClickHouse
        # would otherwise hang an admin request for ever. A normal wait is the
        # length of one Silver tick — measured on production at about three
        # seconds — so this fires only on a genuine hang.
        #
        # Cancelling mid-write is safe: asyncpg cancels the statement and the
        # transaction rolls back, leaving the table exactly as it was.
        async def _locked():
            async with PG_LAYER_LOCK:
                return await ship_order_utm(store, force=force)

        return await asyncio.wait_for(_locked(), timeout=LOCK_WAIT_S)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Order UTM not shipped after a reparse — Postgres keeps the "
            "previous classification until the next Silver tick: %s",
            exc, exc_info=True,
        )
        return {"error": f"{type(exc).__name__}: {exc}"}
