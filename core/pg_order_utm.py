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
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Sequence, Tuple

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


async def ship_order_utm(store, pool=None) -> Dict[str, Any]:
    """Replace `silver.order_utm` from DuckDB. Raises.

    Loud, like `rebuild_silver` beside which it runs and unlike the mirror in
    the sync path: the caller treats a failure of any Postgres layer as a
    reason to leave the whole tick unstamped, and a shipper that reported
    success while writing nothing is the worst available outcome.

    The DuckDB read and the Postgres round trips are in separate blocks. The
    store's lock is not reentrant and every other reader waits behind it, so
    awaiting the network while holding it would stall the dashboard for the
    length of the copy — `replicate_operational`'s rule, and the reason this
    is called from outside every `store.connection()` block.
    """
    from core.pg import get_pool, require_revision
    from core.pg_landing import _WATERMARK_OK, _record_failure

    started = time.monotonic()

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

        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(f"TRUNCATE {UTM_TABLE}")
                await _write_chunked(conn, _insert(), rows)
                # The same watermark table the mirror and the Silver rebuild
                # use, so the comparison can tell "not shipped yet" from
                # "shipped wrong" without a second mechanism to learn.
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

    result = {
        "rows": len(rows),
        "read_ms": read_ms,
        "duration_ms": int((time.monotonic() - started) * 1000),
    }
    logger.info("Order UTM shipped to Postgres: %s", result)
    return result


async def _write_chunked(conn, sql: str, rows: Sequence[tuple]) -> None:
    for start in range(0, len(rows), CHUNK):
        await conn.executemany(sql, rows[start:start + CHUNK])


async def ship_after_reparse(store) -> Dict[str, Any]:
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
                return await ship_order_utm(store)

        return await asyncio.wait_for(_locked(), timeout=LOCK_WAIT_S)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Order UTM not shipped after a reparse — Postgres keeps the "
            "previous classification until the next Silver tick: %s",
            exc, exc_info=True,
        )
        return {"error": f"{type(exc).__name__}: {exc}"}
