"""The bot's state in Postgres — the second adapter behind `core/bot_store.py`.

`app.authorized_users` and its three neighbours have existed since revision
0009 and have been receiving an hourly copy of `data/bot.db` ever since. This
is what lets the bot read and write them instead.

ONE DRIVER, ON A LOOP OF ITS OWN — MEASURED, NOT PREFERRED

The port is synchronous (see `core/bot_store.py` for why), and asyncpg is not.
The alternative was `psycopg`, which this repository has already rejected once
in writing — `requirements-dev.txt` says "two drivers' behaviour to know, for
one database" — so the bar for a second driver was a measurement, and the
measurement did not clear it. On production hardware, from inside a running
event loop, 300 calls each:

    SQLite     read 0.252 ms   write 0.259 ms      what the bot pays today
    bridge     read 0.674 ms   write 1.649 ms      this
    await      read 0.515 ms                       the floor

So the bridge itself costs **0.16 ms**, and Postgres costs about half a
millisecond more than SQLite per call. A handler makes three to five of them
against a Telegram round trip of ~100 ms.

**It blocks the bot's event loop, and that is stated rather than buried.**
Measured: 50 calls took 42 ms and a 1 ms ticker running beside them ticked
**zero** times. The loop is stalled for the whole call. So is it today — SQLite
is a blocking C call — and the change is in duration, from ~0.25 ms to
~0.7–1.6 ms. If the bot ever grows enough traffic for that to matter, the fix
is an async port and forty `await`s, not a second driver.

THE ADAPTER RETURNS SQLITE'S SHAPES, DELIBERATELY

Forty-six call sites were left untouched by the port, so the engine must change
without them noticing. Postgres does not naturally agree with SQLite on two
things, and both would have shipped silently:

* **`notifications_enabled`.** `bot/handlers_legacy.py:1858` writes
  `1 if enabled else 0`, and asyncpg refuses an `int` for a `BOOLEAN` column.
  Writes are coerced here; reads come back as `1`/`0`, which is what
  `core/bot_prefs.py` compares against.
* **Timestamps.** Postgres returns aware `datetime` objects where SQLite
  returns `'YYYY-MM-DD HH:MM:SS'` strings, and the admin screens interpolate
  those straight into a message. Rendered back to SQLite's text on the way out.

Both are pinned by running the same characterisation suite against both
adapters — `tests/unit/test_bot_database.py`, which needs a `KS_PG_DSN` to
include this one.

WHAT IT DOES NOT DO

It does not write `data/bot.db`, and nothing writes both. The hourly copy in
`core/pg_bot_state.py` goes one way — SQLite to Postgres — so turning this
adapter on means turning that copy off, or it will overwrite what the bot
wrote. `KS_BOT_STORE=postgres` is therefore a switch, never a dual-write, and
the guard for it lives in the copy rather than here.
"""
from __future__ import annotations

import asyncio
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from bot.store_sqlite import (
    ALLOWED_PREFERENCES,
    FREEZE_DURATION_DAYS,
    MAX_DENIAL_COUNT,
    STATUS_APPROVED,
    STATUS_DENIED,
    STATUS_FROZEN,
    STATUS_PENDING,
)

logger = logging.getLogger(__name__)

AUTHORIZED = "app.authorized_users"
PREFERENCES = "app.user_preferences"
MILESTONES = "app.celebrated_milestones"

# How long a synchronous caller waits on the loop thread. Long enough that a
# slow query is not turned into a mystery, short enough that a wedged pool does
# not hang a Telegram handler forever.
CALL_TIMEOUT_SECONDS = 30.0

# Columns whose value must look like SQLite's to a caller that has never heard
# of Postgres. Named per column rather than inferred from the value: a `1` in
# `denial_count` is a count, and a `1` here is a decision.
_BOOLEAN_COLUMNS = frozenset({"notifications_enabled"})


class _LoopThread:
    """One event loop, in one daemon thread, owning one pool.

    asyncpg binds a pool to the loop that created it — the trap recorded in the
    Postgres handoff — so the pool is created *on* this loop and used only from
    it. That is the whole reason this class exists rather than an
    `asyncio.run()` per call, which would build and tear down a pool every time
    somebody asked who was approved.
    """

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._run, name="bot-store-postgres", daemon=True,
        )
        self._thread.start()

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def run(self, coro, timeout: float = CALL_TIMEOUT_SECONDS):
        """Await `coro` on the loop thread and hand back its result.

        `run_coroutine_threadsafe` insists on an actual coroutine — an
        awaitable is not enough, and `asyncpg.create_pool()` returns a `Pool`,
        which is awaitable and not a coroutine. Anything passed here must
        already be wrapped in an `async def`; `_open_pool` below is that
        wrapper and the reason this comment exists.
        """
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result(timeout)

    def close(self) -> None:
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=5)
        self._loop.close()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_sqlite_text(value: Any) -> Any:
    """A Postgres moment in the text SQLite would have stored.

    The admin screens interpolate these straight into a message, so a
    `datetime` here becomes `2026-08-01 10:00:00+00:00` on somebody's phone
    where it used to read `2026-08-01 10:00:00`.
    """
    if isinstance(value, datetime):
        moment = value.astimezone(timezone.utc) if value.tzinfo else value
        return moment.strftime("%Y-%m-%d %H:%M:%S")
    return value


def _row(record) -> Optional[Dict[str, Any]]:
    """One asyncpg record in the shapes a SQLite caller expects."""
    if record is None:
        return None
    out: Dict[str, Any] = {}
    for key, value in dict(record).items():
        if key in _BOOLEAN_COLUMNS and isinstance(value, bool):
            out[key] = 1 if value else 0
        else:
            out[key] = _as_sqlite_text(value)
    return out


class PostgresAccessControl:
    def __init__(self, run, pool):
        self._run = run
        self._pool = pool

    def status(self, user_id: int) -> Optional[Dict[str, Any]]:
        return self._run(self._status(user_id))

    async def _status(self, user_id: int) -> Optional[Dict[str, Any]]:
        async with self._pool.acquire() as conn:
            return _row(await conn.fetchrow(
                f"SELECT * FROM {AUTHORIZED} WHERE user_id = $1", user_id,
            ))

    def is_approved(self, user_id: int) -> bool:
        status = self.status(user_id)
        return bool(status) and status["status"] == STATUS_APPROVED

    def is_frozen(self, user_id: int) -> bool:
        return self._run(self._is_frozen(user_id))

    async def _is_frozen(self, user_id: int) -> bool:
        async with self._pool.acquire() as conn:
            record = await conn.fetchrow(
                f"SELECT status, reviewed_at FROM {AUTHORIZED} WHERE user_id = $1",
                user_id,
            )
            if record is None or record["status"] != STATUS_FROZEN:
                return False

            reviewed_at = record["reviewed_at"]
            if reviewed_at is not None:
                # Aware on this side, so no parsing and no chance of the naive
                # comparison that cost SQLite two defects.
                if _utc_now() - reviewed_at > timedelta(days=FREEZE_DURATION_DAYS):
                    await conn.execute(
                        f"""
                        UPDATE {AUTHORIZED}
                        SET status = $1, denial_count = 0
                        WHERE user_id = $2 AND status = $3
                        """,
                        STATUS_DENIED, user_id, STATUS_FROZEN,
                    )
                    logger.info(
                        f"User {user_id} auto-unfrozen after "
                        f"{FREEZE_DURATION_DAYS} days"
                    )
                    return False
            return True

    def request(
        self, user_id: int, username: Optional[str] = None,
        first_name: Optional[str] = None, last_name: Optional[str] = None,
    ) -> bool:
        return self._run(self._request(user_id, username, first_name, last_name))

    async def _request(self, user_id, username, first_name, last_name) -> bool:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                existing = await conn.fetchval(
                    f"SELECT 1 FROM {AUTHORIZED} WHERE user_id = $1", user_id,
                )
                if existing:
                    return False
                await conn.execute(
                    f"""
                    INSERT INTO {AUTHORIZED}
                           (user_id, username, first_name, last_name, status,
                            requested_at, denial_count)
                    VALUES ($1, $2, $3, $4, $5, now(), 0)
                    """,
                    user_id, username, first_name, last_name, STATUS_PENDING,
                )
        logger.info(f"Access request created for user {user_id} (@{username})")
        return True

    def approve(
        self, user_id: int, admin_id: int, *, expected_status: Optional[str] = None,
    ) -> bool:
        return self._run(self._approve(user_id, admin_id, expected_status))

    async def _approve(self, user_id: int, admin_id: int, expected_status) -> bool:
        async with self._pool.acquire() as conn:
            tag = await conn.execute(
                f"""
                UPDATE {AUTHORIZED}
                SET status = $1, reviewed_at = now(), reviewed_by = $2,
                    denial_count = 0
                WHERE user_id = $3 AND ($4::text IS NULL OR status = $4)
                """,
                STATUS_APPROVED, admin_id, user_id, expected_status,
            )
        success = _written(tag)
        if success:
            logger.info(f"User {user_id} approved by admin {admin_id}")
        return success

    def deny(
        self, user_id: int, admin_id: int, *, expected_status: Optional[str] = None,
    ) -> Tuple[bool, bool]:
        return self._run(self._deny(user_id, admin_id, expected_status))

    async def _deny(self, user_id: int, admin_id: int, expected_status) -> Tuple[bool, bool]:
        async with self._pool.acquire() as conn:
            # One transaction, unlike SQLite's read-then-write: the count is
            # what decides the freeze, and two admins denying at once must not
            # both read the same number.
            async with conn.transaction():
                row = await conn.fetchrow(
                    f"SELECT denial_count, status FROM {AUTHORIZED} "
                    "WHERE user_id = $1 FOR UPDATE",
                    user_id,
                )
                if expected_status is not None and (
                    row is None or row["status"] != expected_status
                ):
                    return False, False
                current = row["denial_count"] if row else None
                new_count = (current or 0) + 1
                if new_count >= MAX_DENIAL_COUNT:
                    new_status, is_frozen = STATUS_FROZEN, True
                else:
                    new_status, is_frozen = STATUS_DENIED, False

                tag = await conn.execute(
                    f"""
                    UPDATE {AUTHORIZED}
                    SET status = $1, reviewed_at = now(), reviewed_by = $2,
                        denial_count = $3
                    WHERE user_id = $4
                    """,
                    new_status, admin_id, new_count, user_id,
                )
        success = _written(tag)
        if success:
            if is_frozen:
                logger.info(
                    f"User {user_id} FROZEN by admin {admin_id} "
                    f"(denied {new_count} times)"
                )
            else:
                logger.info(
                    f"User {user_id} denied by admin {admin_id} "
                    f"(count: {new_count}/{MAX_DENIAL_COUNT})"
                )
        return success, is_frozen

    def unfreeze(self, user_id: int, admin_id: int) -> bool:
        return self._run(self._unfreeze(user_id, admin_id))

    async def _unfreeze(self, user_id: int, admin_id: int) -> bool:
        async with self._pool.acquire() as conn:
            tag = await conn.execute(
                f"""
                UPDATE {AUTHORIZED}
                SET status = $1, denial_count = 0, reviewed_at = now(),
                    reviewed_by = $2
                WHERE user_id = $3 AND status = $4
                """,
                STATUS_DENIED, admin_id, user_id, STATUS_FROZEN,
            )
        success = _written(tag)
        if success:
            logger.info(f"User {user_id} unfrozen by admin {admin_id}")
        return success

    def reset_to_pending(self, user_id: int) -> Tuple[bool, bool]:
        if self.is_frozen(user_id):
            logger.warning(
                f"Frozen user {user_id} attempted to re-request access"
            )
            return False, True
        return self._run(self._reset(user_id))

    async def _reset(self, user_id: int) -> Tuple[bool, bool]:
        async with self._pool.acquire() as conn:
            tag = await conn.execute(
                f"""
                UPDATE {AUTHORIZED}
                SET status = $1, requested_at = now(),
                    reviewed_at = NULL, reviewed_by = NULL
                WHERE user_id = $2 AND status = $3
                """,
                STATUS_PENDING, user_id, STATUS_DENIED,
            )
        success = _written(tag)
        if success:
            logger.info(f"User {user_id} reset to pending status")
        return success, False

    def touch(self, user_id: int) -> None:
        self._run(self._touch(user_id))

    async def _touch(self, user_id: int) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                UPDATE {AUTHORIZED}
                SET last_activity = now()
                WHERE user_id = $1 AND status = $2
                """,
                user_id, STATUS_APPROVED,
            )

    def sweep_inactive(self, days: int = 45) -> int:
        return self._run(self._sweep(days))

    async def _sweep(self, days: int) -> int:
        cutoff = _utc_now() - timedelta(days=days)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                doomed = await conn.fetch(
                    f"""
                    SELECT user_id, username FROM {AUTHORIZED}
                    WHERE status = $1 AND (
                        last_activity < $2 OR
                        (last_activity IS NULL AND reviewed_at < $2)
                    )
                    """,
                    STATUS_APPROVED, cutoff,
                )
                if not doomed:
                    return 0
                tag = await conn.execute(
                    f"""
                    UPDATE {AUTHORIZED}
                    SET status = $1
                    WHERE status = $2 AND (
                        last_activity < $3 OR
                        (last_activity IS NULL AND reviewed_at < $3)
                    )
                    """,
                    STATUS_DENIED, STATUS_APPROVED, cutoff,
                )
        for user in doomed:
            logger.info(
                f"Revoked inactive user {user['user_id']} "
                f"(@{user['username']}) - no activity for {days}+ days"
            )
        return _rowcount(tag)

    def _by_status(self, status: str, order: str) -> List[Dict[str, Any]]:
        return self._run(self._list(status, order))

    async def _list(self, status: str, order: str) -> List[Dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM {AUTHORIZED} WHERE status = $1 ORDER BY {order}",
                status,
            )
        return [_row(r) for r in rows]

    def approved(self) -> List[Dict[str, Any]]:
        return self._by_status(STATUS_APPROVED, "reviewed_at DESC")

    def pending(self) -> List[Dict[str, Any]]:
        return self._by_status(STATUS_PENDING, "requested_at ASC")

    def frozen(self) -> List[Dict[str, Any]]:
        return self._by_status(STATUS_FROZEN, "reviewed_at DESC")


class PostgresPreferences:
    def __init__(self, run, pool):
        self._run = run
        self._pool = pool

    def get(self, user_id: int) -> Optional[Dict[str, Any]]:
        return self._run(self._get(user_id))

    async def _get(self, user_id: int) -> Optional[Dict[str, Any]]:
        async with self._pool.acquire() as conn:
            return _row(await conn.fetchrow(
                f"SELECT * FROM {PREFERENCES} WHERE user_id = $1", user_id,
            ))

    def save(
        self, user_id: int, default_source: Optional[str] = None,
        default_report_type: str = "summary", timezone: str = "Europe/Kyiv",
        default_date_range: str = "week", notifications_enabled: bool = True,
    ) -> None:
        self._run(self._save(
            user_id, default_source, default_report_type, timezone,
            default_date_range, notifications_enabled,
        ))
        logger.debug(f"Saved preferences for user {user_id}")

    async def _save(
        self, user_id, default_source, default_report_type, tz,
        default_date_range, notifications_enabled,
    ) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                f"""
                INSERT INTO {PREFERENCES}
                       (user_id, default_source, default_report_type, timezone,
                        default_date_range, notifications_enabled,
                        created_at, updated_at, language)
                VALUES ($1, $2, $3, $4, $5, $6, now(), now(), NULL)
                ON CONFLICT (user_id) DO UPDATE SET
                    default_source = EXCLUDED.default_source,
                    default_report_type = EXCLUDED.default_report_type,
                    timezone = EXCLUDED.timezone,
                    default_date_range = EXCLUDED.default_date_range,
                    notifications_enabled = EXCLUDED.notifications_enabled,
                    updated_at = now()
                """,
                user_id, default_source, default_report_type, tz,
                default_date_range, bool(notifications_enabled),
            )

    def set(self, user_id: int, key: str, value: Any) -> None:
        if not self.get(user_id):
            self.save(user_id)
        if key not in ALLOWED_PREFERENCES:
            logger.warning(f"Attempted to update invalid preference key: {key}")
            return
        self._run(self._set(user_id, key, value))

    async def _set(self, user_id: int, key: str, value: Any) -> None:
        # `bot/handlers_legacy.py` writes `1 if enabled else 0`, and asyncpg
        # refuses an int for a BOOLEAN. Coerced rather than fixed at the call
        # site, because the whole point of the port is that the call sites did
        # not have to know which engine they are talking to.
        if key in _BOOLEAN_COLUMNS:
            value = bool(value)
        async with self._pool.acquire() as conn:
            # `ALLOWED_PREFERENCES` above is what makes the interpolation safe,
            # and it is the same frozenset the SQLite adapter uses.
            await conn.execute(
                f"UPDATE {PREFERENCES} SET {key} = $1, updated_at = now() "
                "WHERE user_id = $2",
                value, user_id,
            )


class PostgresMilestones:
    def __init__(self, run, pool):
        self._run = run
        self._pool = pool

    def already_celebrated(
        self, period_type: str, period_key: str, amount: int,
    ) -> bool:
        return self._run(self._already(period_type, period_key, amount))

    async def _already(self, period_type, period_key, amount) -> bool:
        async with self._pool.acquire() as conn:
            return await conn.fetchval(
                f"""
                SELECT 1 FROM {MILESTONES}
                WHERE period_type = $1 AND period_key = $2
                  AND milestone_amount = $3
                """,
                period_type, period_key, amount,
            ) is not None

    def celebrate(
        self, period_type: str, period_key: str, amount: int, revenue: float,
    ) -> bool:
        return self._run(self._celebrate(period_type, period_key, amount, revenue))

    async def _celebrate(self, period_type, period_key, amount, revenue) -> bool:
        async with self._pool.acquire() as conn:
            # `ON CONFLICT DO NOTHING` rather than SQLite's caught
            # IntegrityError. The id is generated here because Postgres owns it
            # once the bot writes: `MAX(id) + 1` under the same statement, so a
            # second announcement of the same milestone still returns False.
            tag = await conn.execute(
                f"""
                INSERT INTO {MILESTONES}
                       (id, period_type, period_key, milestone_amount,
                        revenue, celebrated_at)
                SELECT COALESCE(MAX(id), 0) + 1, $1, $2, $3, $4, now()
                FROM {MILESTONES}
                ON CONFLICT (period_type, period_key, milestone_amount)
                DO NOTHING
                """,
                period_type, period_key, amount, revenue,
            )
        return _written(tag)

    def recent(
        self, period_type: Optional[str] = None, limit: int = 10,
    ) -> List[Dict[str, Any]]:
        return self._run(self._recent(period_type, limit))

    async def _recent(self, period_type, limit) -> List[Dict[str, Any]]:
        async with self._pool.acquire() as conn:
            if period_type:
                rows = await conn.fetch(
                    f"SELECT * FROM {MILESTONES} WHERE period_type = $1 "
                    "ORDER BY celebrated_at DESC LIMIT $2",
                    period_type, limit,
                )
            else:
                rows = await conn.fetch(
                    f"SELECT * FROM {MILESTONES} "
                    "ORDER BY celebrated_at DESC LIMIT $1",
                    limit,
                )
        return [_row(r) for r in rows]


def _rowcount(tag: str) -> int:
    """`UPDATE 3` — asyncpg hands back the command tag, not a cursor."""
    try:
        return int(str(tag).rsplit(" ", 1)[1])
    except (IndexError, ValueError):
        return 0


def _written(tag: str) -> bool:
    return _rowcount(tag) > 0


class PostgresBotStore:
    """The three persistent aggregates against `app.*`; the cache stays local.

    `cache` is deliberately the SQLite one even here. Revision 0009 left it out
    of Postgres because a ten-minute TTL cache holds nothing that can be lost,
    and a network round trip to fetch something that exists to avoid a network
    round trip is the wrong shape twice over.
    """

    def __init__(self, dsn: Optional[str] = None) -> None:
        from bot.store_sqlite import SqliteCache

        self._dsn = dsn or os.getenv("KS_PG_DSN", "").strip()
        if not self._dsn:
            raise RuntimeError(
                "KS_PG_DSN is not set — refusing to guess a database. "
                "See core/pg.py, which refuses for the same reason."
            )
        self._loop = _LoopThread()
        self._pool = self._loop.run(self._open_pool())

        run, pool = self._loop.run, self._pool
        self.access = PostgresAccessControl(run, pool)
        self.preferences = PostgresPreferences(run, pool)
        self.milestones = PostgresMilestones(run, pool)
        self.cache = SqliteCache()

    async def _open_pool(self):
        """The wrapper `run` insists on.

        `asyncpg.create_pool()` returns a `Pool`, which is awaitable and is not
        a coroutine, and `run_coroutine_threadsafe` raises `TypeError` on it.
        Found by hitting it.
        """
        import asyncpg

        return await asyncpg.create_pool(
            dsn=self._dsn,
            min_size=int(os.getenv("KS_BOT_POOL_MIN", "1")),
            max_size=int(os.getenv("KS_BOT_POOL_MAX", "3")),
        )

    def initialise(self) -> None:
        """Check the revision, and make sure the cache still has a table.

        Nothing to create in Postgres — charter rule 11: Alembic owns that
        schema and the application declares the revision it needs and fails
        closed.

        The SQLite half is not a formality. The cache stays local under this
        engine, and it found this out on a real database: with `data/bot.db`
        absent — a fresh deploy without the bind mount, say — `cache_set`
        raised `no such table: cache` and took a sales report down with it. The
        cache is a performance nicety whose failure is not supposed to be
        anybody's problem, so its table is created here rather than assumed.
        """
        from core.pg import REQUIRED_REVISION

        found = self._loop.run(self._revision())
        if found != REQUIRED_REVISION:
            raise RuntimeError(
                f"bot store: schema is at {found!r}, this code requires "
                f"{REQUIRED_REVISION!r}. Run `alembic upgrade head`."
            )

        from bot.store_sqlite import SqliteBotStore

        SqliteBotStore().initialise()
        logger.info("Bot store: Postgres at %s, cache local", found)

    async def _revision(self) -> Optional[str]:
        async with self._pool.acquire() as conn:
            try:
                return await conn.fetchval(
                    "SELECT version_num FROM meta.alembic_version LIMIT 1"
                )
            except Exception:
                return None

    def stats(self) -> Dict[str, int]:
        return self._loop.run(self._stats())

    async def _stats(self) -> Dict[str, int]:
        async with self._pool.acquire() as conn:
            return {
                "users": await conn.fetchval(
                    f"SELECT COUNT(*) FROM {PREFERENCES}"
                ),
                "reports": await conn.fetchval(
                    "SELECT COUNT(*) FROM app.report_history"
                ),
                "active_cache": self.cache_active_count(),
            }

    def cache_active_count(self) -> int:
        """The cache is still SQLite's, so its count comes from there."""
        from bot.store_sqlite import SqliteBotStore

        try:
            return SqliteBotStore().stats()["active_cache"]
        except Exception:
            return 0

    def close(self) -> None:
        """Shut the loop thread down. For tests and for an orderly exit; the
        thread is a daemon, so a process that forgets still terminates."""
        try:
            self._loop.run(self._pool.close(), timeout=10)
        finally:
            self._loop.close()
