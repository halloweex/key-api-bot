"""The Postgres connection, and the version gate in front of it.

Nothing in this application reads or writes Postgres yet. This is the plumbing
step 05 needs before landing can be mirrored, and it is deliberately inert: the
pool is created on first use and never at import or startup, so a database that
is down, unreachable or not yet migrated changes nothing about the running
system. Charter rule 8 — every step leaves the system working.

**Why asyncpg and not SQLAlchemy.** The sibling repository already runs
`asyncpg==0.30.0` against this same engine, and matching it means one driver's
behaviour to know rather than two. SQLAlchemy arrives anyway as Alembic's
dependency, but only in the dev image: migrations are applied deliberately, by
a person or a deploy step, never by the application on boot.

**Why the application does not migrate itself.** Charter rule 11: the schema
belongs to the application, and the application *declares* the version it needs
and fails closed when the database offers less. Self-healing is what turned the
2026-08-09 incident from one failed `ALTER` into a rebuild every two minutes for
hours — a deploy that arrives before its column must stop, loudly and once.
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

# The DSN. Not `DATABASE_URL`, which is what the sibling bot calls its only
# database: this repository's main store is DuckDB, and an unqualified name
# here would be read as that one by whoever comes next.
DSN_ENV = "KS_PG_DSN"

# Where Alembic keeps its bookkeeping. Named explicitly rather than left to the
# role's `search_path`, so the table cannot silently move when the search_path
# changes — which is the same failure #120 fixed for the application's own DDL.
VERSION_TABLE_SCHEMA = "meta"
VERSION_TABLE = "alembic_version"

# The revision this code requires. Bumped in the same commit as the migration
# that introduces it.
#
# `require_revision` is called by each Postgres operation, **not at startup** —
# the comment here used to claim otherwise, and `web/main.py` does not import
# this module at all. So a deploy carrying new reads against an un-migrated
# database starts normally and then: the mirror of landing keeps working (it
# does not gate), while `rebuild_silver`, `rebuild_gold`, the backfill and all
# three reconciliations raise `SchemaVersionError`. The rebuilds are swallowed
# and logged, the checks persist failed runs, and the layer ages go stale —
# which is what the digest and the canary read. It fails closed and it is not
# silent, but it is the next morning's message, not a crash loop.
REQUIRED_REVISION = "0024_buyer_gender"

_pool: Optional[Any] = None
_pool_lock = asyncio.Lock()


class SchemaVersionError(RuntimeError):
    """The database is not at the revision this code was written against."""


def dsn() -> str:
    """The configured DSN, or a clear failure.

    Raises rather than defaulting to localhost: guessing a database is how you
    migrate the wrong one.
    """
    value = os.getenv(DSN_ENV, "").strip()
    if not value:
        raise RuntimeError(
            f"{DSN_ENV} is not set — refusing to guess a database. "
            f"Expected postgresql://ks_app:...@postgres:5432/ks"
        )
    return value


async def get_pool(**kwargs: Any):
    """The connection pool, created on first use.

    Coroutine-safe: the double-check under a lock is the same shape
    `get_store()` uses, and for the same reason — two concurrent first calls
    would otherwise each build a pool and one would be dropped on the floor
    still holding its connections.
    """
    global _pool
    if _pool is not None:
        return _pool
    async with _pool_lock:
        if _pool is None:
            import asyncpg

            _pool = await asyncpg.create_pool(
                dsn=dsn(),
                # Small on purpose: `max_connections=40` on a 512 MB server
                # shared with a second application and pg_receivewal's slot.
                min_size=int(os.getenv("KS_PG_POOL_MIN", "1")),
                max_size=int(os.getenv("KS_PG_POOL_MAX", "5")),
                command_timeout=float(os.getenv("KS_PG_TIMEOUT", "30")),
                **kwargs,
            )
            logger.info("Postgres pool opened")
    return _pool


async def close_pool() -> None:
    """Close the pool if one was ever opened. Safe to call when none was."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("Postgres pool closed")


async def current_revision(conn=None) -> Optional[str]:
    """The revision Alembic has recorded, or None if it has never run.

    None and "a revision we do not recognise" are different answers and are
    kept apart: the first means nobody has migrated this database, the second
    means somebody migrated it to something else.
    """
    sql = (
        f"SELECT version_num FROM {VERSION_TABLE_SCHEMA}.{VERSION_TABLE} LIMIT 1"
    )
    if conn is not None:
        try:
            return await conn.fetchval(sql)
        except Exception:
            return None
    pool = await get_pool()
    async with pool.acquire() as acquired:
        try:
            return await acquired.fetchval(sql)
        except Exception:
            return None


async def require_revision(expected: str = REQUIRED_REVISION, conn=None) -> None:
    """Fail closed unless the database is at `expected`.

    Charter rule 11, as a callable. Deliberately raises on *any* mismatch and
    not only on "older": Alembic revisions are opaque strings with no order to
    compare, and a database ahead of this code is as much a reason to stop as
    one behind it — the code is about to read columns whose meaning it does not
    know.
    """
    found = await current_revision(conn)
    if found == expected:
        return
    if found is None:
        raise SchemaVersionError(
            f"{VERSION_TABLE_SCHEMA}.{VERSION_TABLE} is absent or empty: this "
            f"database has never been migrated. Run `alembic upgrade head`."
        )
    raise SchemaVersionError(
        f"schema is at {found!r}, this code requires {expected!r}. "
        f"Run `alembic upgrade head`, or deploy the code that matches."
    )
