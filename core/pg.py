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
# `require_revision` is called by each Postgres operation, not by web at
# startup (`web/main.py` does not import this module). What a mismatch costs has
# grown with every port, and is no longer "the next morning's message": under
# `KS_USER_STORE=postgres` the session read raises, so the dashboard answers 401
# to everybody; the bot store checks it in `initialise()` and the bot refuses to
# start; every write chain, the buyers mirror and the paths that stand down on
# owner rows, the replication, the derivation and the reconciliations raise
# `SchemaVersionError` and record it. Three writers do not gate: the catalogue
# mirror of landing (`pg_landing._mirror`), the per-tick orders mirror
# (`upsert_orders` → `mirror_orders` → `write_orders`) and the alert journal,
# which stays up on purpose. Compose runs `migrate` first and makes web and bot
# wait for it, which is what keeps a deploy out of that state.
#
# 0034 is chain 4's lock (owner decision 16): an image that does not know the
# buyers chain refuses a database that does. See the revision's docstring for
# what the lock does not cover — images below v3.0.249 — and why an image-only
# rollback after it needs `alembic downgrade` first, and none after the flip.
#
# 0035 is the same lock for the eight chains #280 registered — 3, 5, 6, 7b-3,
# 9, 10, 11a, 11b — shipped before the first of their flips. Its docstring says
# what it does not hold: the per-tick landing mirrors, which never gated, and
# DuckDB. An image-only rollback below it needs `alembic downgrade
# 0034_buyer_chain` with the new migrate image first, and none once one of
# those chains has latched. Each of the eight names it as its
# `LOCKOUT_REVISION`, and its flag moves nothing in a build that requires less
# (`core.chain_latch.lockout_unmet`).
REQUIRED_REVISION = "0035_batch_e_chains"

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


async def current_revision(conn=None, *, strict: bool = False) -> Optional[str]:
    """The revision Alembic has recorded, or None if it has never run.

    None and "a revision we do not recognise" are different answers and are
    kept apart: the first means nobody has migrated this database, the second
    means somebody migrated it to something else.

    By default None also stands for a read that failed — any exception from
    the SELECT. `strict=True` keeps None for the one thing it says, the
    version table absent (or empty), and lets every other failure raise: a
    connection lost in the middle of the read is not a database nobody
    migrated, and a caller that asks again must be able to tell them apart
    (`core.warehouse_cutover`, where after a flip that reading is the way
    back).
    """
    sql = (
        f"SELECT version_num FROM {VERSION_TABLE_SCHEMA}.{VERSION_TABLE} LIMIT 1"
    )
    if conn is not None:
        return await _fetch_revision(conn, sql, strict)
    pool = await get_pool()
    async with pool.acquire() as acquired:
        return await _fetch_revision(acquired, sql, strict)


async def _fetch_revision(conn, sql: str, strict: bool) -> Optional[str]:
    if not strict:
        try:
            return await conn.fetchval(sql)
        except Exception:
            return None
    import asyncpg

    try:
        return await conn.fetchval(sql)
    except (asyncpg.UndefinedTableError, asyncpg.InvalidSchemaNameError):
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
