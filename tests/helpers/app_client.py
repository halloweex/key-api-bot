"""The app called the way a visitor calls it, over the test Postgres.

`TestClient(web.main.app)` is built without its context manager, so no
startup job runs. The client runs each request on an event loop of its own,
which a pool made in the test's loop does not belong to, so `core.pg.get_pool`
answers with `ConnectionPerAcquire`: a fresh connection per `acquire()`, on
whichever loop asks.

The visitor holds a real signed session (`tests.sessions.session_cookie`) and
is a row of `app.dashboard_users`, which `_resolve_session` reads on every
request as it does in production: role, status and tab set. A hardcoded admin
needs no row, since that is the one account the app never looks up.

Seed first, then sign in, inside one async test:

    async with tab_in_postgres(DSN, monkeypatch, populate):
        async with signed_in("viewer") as visitor:
            r = await visitor.get("/api/customers/insights", params={...})

`tab_in_postgres` (`tests/integration/pg_tab_seed.py`) seeds Bronze and
derives Silver and Gold the way the derivation does.
"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional
from unittest.mock import AsyncMock, patch

from tests.sessions import hardcoded_admin, session_cookie

DSN = os.getenv("KS_PG_DSN")

# One account per role, with ids no hardcoded admin has, so that two
# visitors in one test are two rows.
VISITOR_IDS = {"viewer": 7_000_000_001, "editor": 7_000_000_002,
               "admin": 7_000_000_003}


class ConnectionPerAcquire:
    """`core.pg.get_pool`'s answer for an app served by `TestClient`: every
    acquire opens a connection of its own in whichever loop asks, so the
    client's loop and the test's never share one, and nothing is left bound
    to a loop that has closed. The pool's own shortcuts (`fetch`, `execute`
    and the rest) go through it too."""

    def __init__(self, dsn: str):
        self._dsn = dsn

    @asynccontextmanager
    async def acquire(self, *_a, **_k):
        import asyncpg

        conn = await asyncpg.connect(self._dsn)
        try:
            yield conn
        finally:
            await conn.close()

    async def _run(self, method: str, *args, **kwargs):
        async with self.acquire() as conn:
            return await getattr(conn, method)(*args, **kwargs)

    async def fetch(self, *args, **kwargs):
        return await self._run("fetch", *args, **kwargs)

    async def fetchrow(self, *args, **kwargs):
        return await self._run("fetchrow", *args, **kwargs)

    async def fetchval(self, *args, **kwargs):
        return await self._run("fetchval", *args, **kwargs)

    async def execute(self, *args, **kwargs):
        return await self._run("execute", *args, **kwargs)


class Visitor:
    """A signed-in `TestClient`, called from the test's loop. Each request
    runs in a worker thread, so the app's loop never meets the test's.
    `client` is the `TestClient` itself, for anything not wrapped here."""

    def __init__(self, client, user_id: int):
        self.client = client
        self.user_id = user_id

    async def request(self, method: str, url: str, **kwargs: Any):
        return await asyncio.to_thread(self.client.request, method, url, **kwargs)

    async def get(self, url: str, **kwargs: Any):
        return await self.request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: Any):
        return await self.request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs: Any):
        return await self.request("PUT", url, **kwargs)

    async def patch(self, url: str, **kwargs: Any):
        return await self.request("PATCH", url, **kwargs)

    async def delete(self, url: str, **kwargs: Any):
        return await self.request("DELETE", url, **kwargs)


_ACCOUNT_SQL = """
    INSERT INTO app.dashboard_users (user_id, first_name, role, status, allowed_features)
    VALUES ($1, 'Test', $2, 'approved', $3)
    ON CONFLICT (user_id) DO UPDATE SET
        role = excluded.role, status = excluded.status,
        allowed_features = excluded.allowed_features
"""


@asynccontextmanager
async def signed_in(
    role: str = "admin",
    *,
    user_id: Optional[int] = None,
    tabs: Optional[str] = None,
    dsn: Optional[str] = DSN,
    raise_server_exceptions: bool = True,
) -> AsyncIterator[Visitor]:
    """A visitor signed in as `role`, the app reading the test Postgres.

    `tabs` is the account's `allowed_features` as stored: None for "as the
    role", "" for none, "traffic,marketing" for exactly those. `role="admin"`
    with neither `user_id` nor `tabs` is a hardcoded admin, who has no row;
    any other visitor is approved in `app.dashboard_users` for the block and
    removed after it. The permission cache and the rate limiter start and
    end empty.

    `raise_server_exceptions=False` answers a server error as a 500 instead
    of raising it into the test.
    """
    import asyncpg
    from fastapi.testclient import TestClient

    from core.permissions import invalidate_permissions_cache
    from web.main import app
    from web.ratelimit import limiter

    if not dsn:
        raise RuntimeError("signed_in needs a live PostgreSQL at KS_PG_DSN")
    hardcoded = role == "admin" and user_id is None and tabs is None
    uid = hardcoded_admin() if hardcoded else (user_id or VISITOR_IDS[role])

    async def _account(sql: str, *args) -> None:
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(sql, *args)
        finally:
            await conn.close()

    if not hardcoded:
        await _account(_ACCOUNT_SQL, uid, role, tabs)
    invalidate_permissions_cache()
    limiter.reset()
    client = TestClient(app, raise_server_exceptions=raise_server_exceptions)
    name, value = session_cookie(uid, role).split("=", 1)
    client.cookies.set(name, value)
    try:
        with patch("core.pg.get_pool", new=AsyncMock(return_value=ConnectionPerAcquire(dsn))):
            yield Visitor(client, uid)
    finally:
        limiter.reset()
        invalidate_permissions_cache()
        if not hardcoded:
            await _account("DELETE FROM app.dashboard_users WHERE user_id = $1", uid)
