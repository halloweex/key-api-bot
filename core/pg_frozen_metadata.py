"""DuckDB's `sync_metadata` as Postgres last copied it — what `KS_DUCKDB=off` inherits.

Four places read DuckDB's `sync_metadata` for a key Postgres does not hold
yet, because a value that stopped moving at a chain's flip is still the right
stand-in for one that has not been written since (stage 5's design, 2.5):

1. `DuckDBStore.get_last_sync_time` — the sync window of a chain declaring
   `CHAIN_WATERMARK_INHERITS_DUCKDB` (3, 6, 6a);
2. `core.data_quality._freshness_check` — the same stand-in, judged, and the
   bootstrap gate ("no sync history at all");
3. `core.dq_journal.digest_inputs` — the digest's beat;
4. `core.report_ledger.already_sent` — adopting a week delivered before the
   flip (which, under `off`, does not read anything: below).

Under `KS_DUCKDB=off` the file may not be opened, and none of the four may
answer "absent" instead: for `last_sync_orders` that is the window "an hour
ago", the defect chain 3's review found (orders KeyCRM updated between the
stamps are never fetched), and `last_sync_categories` and
`last_sync_expense_types` move only on the Sunday full sync, so Postgres may
lack them for a week after a flip. So the first three read **`app.sync_metadata`
in Postgres** — the hourly full copy of the same DuckDB table
(`core/pg_operational.py`, `SYNC_METADATA_SOURCE`). Once every chain is
latched, DuckDB's values for those keys no longer move, so the copy equals
the original — provided it ran after the last latch, which is the `off`
verdict's `sync_metadata_copied` key (PR-2), not this module's to judge.

The fourth reads nothing under `off`: the authority is the chain's own
`find_sent` in Postgres, and the weeks delivered before the flips of 11a and
11b reached Postgres by the hourly copy before those flips.

Reads only, and only the copy: nothing here writes `app.sync_metadata`, whose
one writer stands down under `off` with the rest of what is copied out of
DuckDB (PR-7). Every caller is behind `duckdb_switch.is_off()`; under `on`
nothing here is asked.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, Optional

# The table, named once. `core.pg_operational.SYNC_METADATA_TABLE` is its
# writer's spelling; a test holds the two to one string.
TABLE = "app.sync_metadata"

_READ_KEYS = f"SELECT key, value FROM {TABLE} WHERE key = ANY($1::text[])"
_READ_PREFIX = f"SELECT key, value FROM {TABLE} WHERE key LIKE $1"


async def _pool():
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    return pool


async def values_on(conn: Any, keys: Iterable[str]) -> Dict[str, str]:
    """`{key: raw value}` for the given keys the copy holds, on a connection
    the caller already has. Absent keys are absent, as in `sync_metadata`."""
    keys = sorted(set(keys))
    if not keys:
        return {}
    rows = await conn.fetch(_READ_KEYS, keys)
    return {r["key"]: r["value"] for r in rows}


async def frozen_values(keys: Iterable[str]) -> Dict[str, str]:
    """`{key: raw value}` for the given keys `app.sync_metadata` holds — raw,
    in the shape `sync_metadata` rows have, so a caller parses them as it
    parsed DuckDB's. Raises when Postgres cannot be read: the caller's own
    handling decides, as it did for DuckDB."""
    keys = sorted(set(keys))
    if not keys:
        return {}
    pool = await _pool()
    async with pool.acquire() as conn:
        return await values_on(conn, keys)


async def frozen_value(key: str) -> Optional[str]:
    """One key's raw value, or None while the copy does not hold it."""
    return (await frozen_values((key,))).get(key)


async def frozen_stamp(key: str) -> Optional[datetime]:
    """One key as `get_last_sync_time` returns it: a datetime, or None."""
    raw = await frozen_value(key)
    return datetime.fromisoformat(raw) if raw else None


async def frozen_sync_rows() -> Dict[str, str]:
    """Every `last_sync_*` key the copy holds — what `_freshness_check` reads
    out of DuckDB's `sync_metadata` under `on`, read out of the copy under
    `off`."""
    pool = await _pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(_READ_PREFIX, "last\\_sync\\_%")
    return {r["key"]: r["value"] for r in rows}
