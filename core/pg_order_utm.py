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


def _insert() -> str:
    values = ", ".join(f"${i}" for i in range(1, len(UTM_COLUMNS) + 1))
    return f"INSERT INTO {UTM_TABLE} ({', '.join(UTM_COLUMNS)}) VALUES ({values})"


def read_utm(conn) -> List[tuple]:
    """Every UTM row out of DuckDB, in the column order Postgres wants.

    Ordered by the key so a failed run and its successor ship the same rows in
    the same sequence, which makes a partial write comparable to a whole one.
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
    from core.pg_landing import _WATERMARK_OK

    started = time.monotonic()
    pool = pool or await get_pool()
    await require_revision()

    async with store.connection() as conn:
        rows = read_utm(conn)

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f"TRUNCATE {UTM_TABLE}")
            await _write_chunked(conn, _insert(), rows)
            # The same watermark table the mirror and the Silver rebuild use,
            # so the comparison can tell "not shipped yet" from "shipped
            # wrong" without a second mechanism to learn.
            await conn.execute(_WATERMARK_OK, UTM_TABLE, len(rows))

    result = {
        "rows": len(rows),
        "duration_ms": int((time.monotonic() - started) * 1000),
    }
    logger.info("Order UTM shipped to Postgres: %s", result)
    return result


async def _write_chunked(conn, sql: str, rows: Sequence[tuple]) -> None:
    for start in range(0, len(rows), CHUNK):
        await conn.executemany(sql, rows[start:start + CHUNK])
