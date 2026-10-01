"""Which buyers the hourly sync fetches, decided from Postgres.

`get_missing_buyer_ids` picks the buyers KeyCRM should be asked for: ids that
orders reference and `buyers` does not hold, or holds without a name. It reads
`silver_orders LEFT JOIN buyers` in DuckDB — so its answer is downstream of
DuckDB's Silver, not of the orders themselves. The chain-2 design review found
it: once DuckDB stops being derived, the selection freezes, returns nothing new
for ever, and `sync_missing_buyers` records "no missing buyers" every hour.
New customers would stop landing and the log would call it a quiet hour.

Postgres' `silver.orders` and `bronze.buyers` are the same facts — the buyers
mirror is fed the same parsed batch as DuckDB — and they keep moving. There is
one thing Postgres sees that DuckDB cannot: a buyer DuckDB stored and the mirror
failed to ship stays "missing" here, so the next hour fetches it again and the
mirror gets another try. From DuckDB that buyer looked done for good.

WHY A FLAG OF ITS OWN, AND NO FALLBACK

The flag ships off, for every port's reason: compare on live data before moving.
There is no fallback because a fallback's answer is exactly the frozen one this
exists to escape. A failure is instead contained by the caller:
`sync_missing_buyers` logs it and returns without moving the buyers watermark,
so the hourly retry stands and `freshness_buyers` says so after 48 hours — and
the offers and stocks syncs after it in the same tick still run. A read refused
under `KS_READ_FALLBACK=off` is recorded the same way and then passed on: the
tick names it and skips the step, and the manual route answers 503 (DN-20c).
"""
from __future__ import annotations

import logging
import os
from typing import Any, List, Sequence, Tuple

logger = logging.getLogger(__name__)

ENV = "KS_READ_BUYER_SYNC"
_VALID = ("duckdb", "postgres")


def enabled() -> bool:
    """Whether the buyer selection should ask Postgres. Unknown values raise."""
    value = os.getenv(ENV, "duckdb").strip().lower() or "duckdb"
    if value not in _VALID:
        raise ValueError(
            f"{ENV}={value!r} — unknown engine. Expected one of {_VALID}; "
            f"a typo here must stop the read, not point it at the other store."
        )
    return value == "postgres"


def available() -> bool:
    """Postgres is configured at all."""
    return bool(os.getenv("KS_PG_DSN", "").strip())


async def fetch(sql: str, params: Sequence[Any] = ()) -> List[Tuple]:
    """Run the selection against Postgres; `sql` arrives rendered, with `?`."""
    from core.pg import get_pool, require_revision
    from core.sql_dialect import numbered

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        rows = await conn.fetch(numbered(sql), *params)
    return [tuple(r) for r in rows]


async def count_buyers() -> int:
    """How many buyers Postgres holds — sync-all's before and after, once
    chain 4 writes the buyers there and DuckDB's count has stopped moving.
    Here rather than in the chain module, whose every public function reaching
    a connection is held to the latch."""
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        return int(await conn.fetchval("SELECT count(*) FROM bronze.buyers"))


async def watermark_age_s(now=None) -> "int | None":
    """Seconds since the buyers step last completed, from the stamp it writes
    to `meta.chain_watermarks` under chain 4 — a clock that survives a web
    restart, where `buyer_sync.last_ok_age_s` is floored at the process start
    and so reads young after every recreate. None when the key is not there
    yet or holds no timestamp. Raises on a store that cannot be read; the
    health block publishes None then."""
    from datetime import datetime, timezone

    from core.pg_chain_watermarks import read_values

    raw = (await read_values(["last_sync_buyers"])).get("last_sync_buyers")
    if not raw:
        return None
    try:
        stamp = datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    return max(0, int((now - stamp).total_seconds()))
