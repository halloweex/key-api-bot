"""Reading the managers from Postgres, once chain 5 writes them there.

`GET /api/managers` and the retail-status route's existence check read
`DuckDBStore.get_all_managers`, which had no read switch at all: the managers
screen is an admin page and DuckDB was where they were written. Under chain 5
(`core/pg_managers_write.py`) DuckDB's `managers` stops moving — nothing writes
it and the replica stands down — so the read follows the writes, whatever any
flag says, and **does not fall back**: DuckDB's copy after the flip is not an
older answer but a frozen one, and an admin reclassifying a manager against it
would be deciding from a list that no longer changes (chain 7a's rule,
`pg_goals_write.reads_the_chain`). A Postgres failure raises, so the page
fails where it can be seen.

No handler here on purpose: `tests/unit/test_read_fallback_sites.py` walks
every handler in `core/pg_*_read*.py`, and this module has nothing to fall
back to.
"""
from __future__ import annotations

from typing import Any, Dict, List

# `synced_at` is DuckDB's bookkeeping; Postgres's is `mirrored_at`, which the
# chain's writer moves on every write of the row — the same meaning, under the
# name the screen already reads.
_ALL_MANAGERS_SQL = """
SELECT id, name, email, status, is_retail,
       first_order_date, last_order_date, order_count,
       mirrored_at AS synced_at
FROM bronze.managers
ORDER BY order_count DESC NULLS LAST, name
"""


async def fetch_all_managers() -> List[Dict[str, Any]]:
    """`DuckDBStore.get_all_managers`' shape, out of `bronze.managers`."""
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        rows = await conn.fetch(_ALL_MANAGERS_SQL)
    return [dict(r) for r in rows]
