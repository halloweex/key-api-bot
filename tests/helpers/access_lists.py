"""The dashboard's and the bot's access lists in the test Postgres.

Two lists decide who gets in: `app.dashboard_users`, which the session reads
on every request, and `app.authorized_users`, the bot's approval queue, with
the bot's preferences beside it in `app.user_preferences`. Production runs
the bot's store on Postgres (`KS_BOT_STORE=postgres`, `bot/store_postgres.py`),
and `bot_store_on_postgres` installs that adapter over the suite's database,
so a route that reads or writes the bot's list does it in the tables a test
reads back.

Nest `tests.helpers.app_client.signed_in` inside it to call the app:

    async with bot_store_on_postgres(monkeypatch, tmp_path):
        await execute("INSERT INTO app.authorized_users ...")
        async with signed_in("admin") as admin:
            r = await admin.post(...)
        assert await rows("SELECT ... FROM app.dashboard_users") == [...]
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List

DSN = os.getenv("KS_PG_DSN")

# Both lists, the role matrix and the bot's preferences: emptied before and
# after each block.
ACCESS_LIST_TABLES = (
    "app.dashboard_users", "app.role_permissions",
    "app.authorized_users", "app.user_preferences",
)


@asynccontextmanager
async def bot_store_on_postgres(monkeypatch, tmp_path) -> AsyncIterator[Any]:
    """The bot's store on the test Postgres for one block, the access tables
    empty before and after it. The store's local cache file goes to
    `tmp_path`, never `data/`."""
    from bot import store_sqlite
    from bot.store_postgres import PostgresBotStore
    from core.bot_store import use_bot_store
    from tests.integration.pg_sms_seed import on_postgres

    if not DSN:
        raise RuntimeError("bot_store_on_postgres needs a live PostgreSQL at KS_PG_DSN")
    monkeypatch.setenv("KS_BOT_STORE", "postgres")
    monkeypatch.setattr(store_sqlite, "DB_PATH", tmp_path / "bot.db")
    async with on_postgres(monkeypatch, ACCESS_LIST_TABLES, what="the access path"):
        store = PostgresBotStore(DSN)
        previous = use_bot_store(store)
        try:
            yield store
        finally:
            use_bot_store(previous)
            store.close()


async def execute(sql: str, *args: Any) -> None:
    """One statement against the test Postgres, on a connection of its own."""
    import asyncpg

    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute(sql, *args)
    finally:
        await conn.close()


async def rows(sql: str, *args: Any) -> List[Dict[str, Any]]:
    """A query's rows as dicts, on a connection of its own."""
    import asyncpg

    conn = await asyncpg.connect(DSN)
    try:
        return [dict(r) for r in await conn.fetch(sql, *args)]
    finally:
        await conn.close()


async def pending_request(user_id: int, *, username: str = "asker",
                          first_name: str = "Ask", denial_count: int = 0) -> None:
    """A person waiting in the bot's queue, as `/start` leaves them."""
    await execute(
        "INSERT INTO app.authorized_users (user_id, username, first_name, "
        "last_name, status, requested_at, denial_count) "
        "VALUES ($1, $2, $3, 'Er', 'pending', now(), $4)",
        user_id, username, first_name, denial_count)


async def dashboard_row(user_id: int) -> Dict[str, Any] | None:
    """The person's row in the dashboard's list, or None."""
    found = await rows(
        "SELECT user_id, role, status, allowed_features, denial_count "
        "FROM app.dashboard_users WHERE user_id = $1", user_id)
    return found[0] if found else None


__all__ = [
    "ACCESS_LIST_TABLES", "DSN", "bot_store_on_postgres", "dashboard_row",
    "execute", "pending_request", "rows",
]
