"""Who may open the dashboard, in Postgres — and the switch that decides.

Revision 0016's one table, filled and then taken over. This is the last read
`_resolve_session` makes, and therefore the last thing standing between a
dashboard request and DuckDB's process-wide store lock: every authenticated
request to every tab acquires it once, so a warehouse rebuild holding that lock
for a second holds up the whole dashboard, not one query. Measured on the SMS
side, that contention took a path from 124 req/s to 38.

WHY THE WRITER MOVES WITH THE READER

`replicate_bot_state` and `replicate_sms` copy a table and let the reader
switch later. That shape is wrong here. `_resolve_session` re-reads status and
role on **every** request, and that read *is* revocation: the session cookie is
a stateless signed token that cannot be withdrawn, so taking access away means
the next request finds the account no longer approved. Reading an hourly
replica would hand a revoked account up to an hour more, which trades a lock
for a security hole.

There is exactly one writer — the web container, through the admin page — so
moving it costs less than making a lagging copy safe. The replication below
exists to carry the 24 existing rows across before the switch, and stands down
after it, for `replicate_sms`' reason: a full replace out of a frozen DuckDB
would undo every approval made since.

WHAT IS DELIBERATELY NOT DONE HERE

The two access lists are not merged. `app.authorized_users` is the bot's and
disagreed with this one about twelve of sixteen people on the day this was
written — see revision 0016 for the numbers and why that is not drift. Whether
they should be one list is the owner's decision; this module moves one of them
between engines and changes neither.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)

TABLE = "app.dashboard_users"

# The columns both stores hold and compare. Bookkeeping is excluded by
# construction: Postgres stamps `mirrored_at` and DuckDB has no counterpart.
USER_COLUMNS: Tuple[str, ...] = (
    "user_id", "username", "first_name", "last_name", "photo_url",
    "role", "status", "requested_at", "reviewed_at", "reviewed_by",
    "last_activity", "denial_count", "created_at", "allowed_features",
)

_ENV = "KS_USER_STORE"
_VALID = ("duckdb", "postgres")


def user_store_is_postgres() -> bool:
    """Which store owns the dashboard's user list.

    An unknown value raises — `KS_BOT_STORE`'s rule, and it matters more here
    than anywhere it has been applied before: a typo in the variable that
    decides where *authorisation* is read from must stop the container, not
    quietly point it at a copy that is about to go stale and start answering
    "approved" for someone who is not.
    """
    value = os.getenv(_ENV, "duckdb").strip().lower() or "duckdb"
    if value not in _VALID:
        raise ValueError(
            f"{_ENV}={value!r} — unknown store. Expected one of {_VALID}; a "
            f"typo in the variable that decides who may open the dashboard "
            f"must stop the container rather than pick a store for you."
        )
    return value == "postgres"


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


# ─── carrying the list across, once ──────────────────────────────────────────


async def replicate_dashboard_users(store) -> Dict[str, Any]:
    """Copy DuckDB's `users` into Postgres. Never raises.

    Full replace, one transaction. The table is 24 rows, an approval can be
    *deleted* by nothing but is a decision either way, and a replace makes "in
    DuckDB and not in Postgres" mean exactly one thing: lost.

    Refuses to run once Postgres is the writer — `replicate_sms`' guard, and
    the same reasoning with a sharper edge: an hourly full replace out of a
    frozen DuckDB would roll back every approval and every role change made
    since the switch, once an hour, looking healthy in between.
    """
    from core.mirror_reconciliation import configured

    if not configured():
        return {"skipped": "KS_PG_DSN is not set"}
    if user_store_is_postgres():
        return {"skipped": f"{_ENV}=postgres — DuckDB is no longer the writer"}

    started = time.monotonic()
    try:
        from core.pg import get_pool, require_revision
        from core.pg_landing import _WATERMARK_OK

        pool = await get_pool()
        await require_revision()

        # The DuckDB read and the Postgres round trip are separate blocks: the
        # store lock is not reentrant and every other reader waits behind it.
        async with store.connection() as conn:
            rows = conn.execute(
                f"SELECT {', '.join(USER_COLUMNS)} FROM users ORDER BY user_id"
            ).fetchall()
        rows = [tuple(r) for r in rows]

        placeholders = ", ".join(f"${i}" for i in range(1, len(USER_COLUMNS) + 1))
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(f"DELETE FROM {TABLE}")
                if rows:
                    await conn.executemany(
                        f"INSERT INTO {TABLE} ({', '.join(USER_COLUMNS)}) "
                        f"VALUES ({placeholders})",
                        rows,
                    )
                await conn.execute(_WATERMARK_OK, TABLE, len(rows))

        elapsed = time.monotonic() - started
        logger.info("Dashboard users replicated: %d rows in %.3fs", len(rows), elapsed)
        return {"rows": len(rows), "elapsed_s": round(elapsed, 3)}
    except Exception as exc:  # noqa: BLE001 — a copy must not take the tick down
        logger.error("Dashboard user replication failed: %s", exc, exc_info=True)
        return {"error": f"{type(exc).__name__}: {exc}"}
