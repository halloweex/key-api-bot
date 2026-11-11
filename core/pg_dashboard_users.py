"""Who may open the dashboard, in Postgres — and the switch that decides.

Revision 0016's one table, filled and then taken over. This is the last read
`_resolve_session` makes, and therefore the last thing standing between a
dashboard request and DuckDB's process-wide store lock: every authenticated
request to every tab acquires it once, so a warehouse rebuild holding that lock
for a second holds up the whole dashboard, not one query. Measured on the SMS
side, that contention took a path from 124 req/s to 38.

WHY THE WRITER MOVES WITH THE READER

`replicate_bot_state` and `replicate_sms` (until step 14's S14-13) copy a
table and let the reader switch later. That shape is wrong here. `_resolve_session` re-reads status and
role on **every** request, and that read *is* revocation: the session cookie is
a stateless signed token that cannot be withdrawn, so taking access away means
the next request finds the account no longer approved. Reading an hourly
replica would hand a revoked account up to an hour more, which trades a lock
for a security hole.

There is exactly one writer — the web container, through the admin page — so
moving it costs less than making a lagging copy safe. A replication carried the
24 existing rows across before the switch and stood down after it, for
`replicate_sms`' reason: a full replace out of a frozen DuckDB would undo every
approval made since. Both copies went in step 14's S14-13.

WHAT IS DELIBERATELY NOT DONE HERE

The two access lists are not merged. `app.authorized_users` is the bot's and
disagreed with this one about twelve of sixteen people on the day this was
written — see revision 0016 for the numbers and why that is not drift. Whether
they should be one list is the owner's decision; this module moves one of them
between engines and changes neither.
"""
from __future__ import annotations

import logging
from typing import Any, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

TABLE = "app.dashboard_users"

# The columns both stores hold and compare. Bookkeeping is excluded by
# construction: Postgres stamps `mirrored_at` and DuckDB has no counterpart.
USER_COLUMNS: Tuple[str, ...] = (
    "user_id", "username", "first_name", "last_name", "photo_url",
    "role", "status", "requested_at", "reviewed_at", "reviewed_by",
    "last_activity", "denial_count", "created_at", "allowed_features",
)

# `user_store_is_postgres()` stood here until step 14's S14-18: which store
# owned the dashboard's user list, Postgres, a constant since S14-00 —
# production since 2026-09-07. Its last reader, the table-fate manifest's
# `duckdb_written`, went in S14-18. `KS_USER_STORE` is read by nothing; keep
# `postgres` in `.env` for the images before step 14, which read an absent
# value as `duckdb` — a 401 for everybody.


# ─── the pool, thinly ────────────────────────────────────────────────────────
#
# `core/pg_sms_read.py` has three functions shaped exactly like these. They are
# not shared, on purpose: charter rule 1 is about a *rule with two homes* — a
# SQL body, a threshold, a projection — and these are five lines of pool
# handling with no rule in them. What would be gained by sharing is outweighed
# by an authentication path that can be broken by a change to the SMS tab.


async def fetch_rows(sql: str, params: Sequence[Any] = ()) -> List[Tuple]:
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        return [tuple(r) for r in await conn.fetch(sql, *params)]


async def fetch_row(sql: str, params: Sequence[Any] = ()) -> Optional[Tuple]:
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, *params)
    return tuple(row) if row is not None else None


async def execute(sql: str, params: Sequence[Any] = ()) -> None:
    from core.pg import get_pool, require_revision

    pool = await get_pool()
    await require_revision()
    async with pool.acquire() as conn:
        await conn.execute(sql, *params)
