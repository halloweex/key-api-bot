"""ClickHouse for the reconciliation tests (batch 5, recon).

CI has no ClickHouse, so most of these tests meet it at its HTTP transport:
`clickhouse_at_the_transport` answers every request the real
`core.ch_common.execute` makes from a `FakeClickHouse`, which holds the
archive copy as a list of ids and refuses the statements a test names.
`CLICKHOUSE_DOWN` is an address nothing listens on: a ClickHouse that is
down, the connection refused by the real client. Postgres is the test
database in all of them.

The two-engine tests need the real server: `needs_clickhouse` skips them
without `KS_CH_URL` (point it at a throwaway 24.8, production's version), and
`fresh_clickhouse_copies` drops the copies the ship creates only when absent.
"""
from __future__ import annotations

import os
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncIterator, Iterable, Optional
from unittest.mock import AsyncMock, patch

import asyncpg
import httpx
import pytest

DSN = os.getenv("KS_PG_DSN")

needs_clickhouse = pytest.mark.skipif(
    not os.getenv("KS_CH_URL"), reason="needs a live ClickHouse at KS_CH_URL")

CLICKHOUSE_DOWN = "http://127.0.0.1:9"


class FakeClickHouse:
    """ClickHouse as the archive's comparison and the Silver ship meet it.

    `archive` is the ids `history.order_versions` holds: the copy's
    high-water mark and its per-10 000-id bucket counts are answered from
    them. A statement naming one of the `refuse` words is answered as a
    failing server answers it, HTTP 500. Anything else (DDL, TRUNCATE, an
    INSERT, EXCHANGE, a count of an empty table) succeeds and holds nothing.
    """

    def __init__(self, archive: Iterable[int] = (), refuse: Iterable[str] = ()):
        self.archive = sorted(archive)
        self.refuse = tuple(word.lower() for word in refuse)

    def handle(self, request: httpx.Request) -> httpx.Response:
        sql = (request.url.params.get("query") or request.content.decode("utf-8")).lower()
        if any(word in sql for word in self.refuse):
            return httpx.Response(
                500, text="Code: 241. DB::Exception: Memory limit (total) exceeded")
        if "group by" in sql:
            buckets = Counter(i // 10_000 for i in self.archive)
            return httpx.Response(
                200, text="".join(f"{b}\t{n}\n" for b, n in sorted(buckets.items())))
        if "max(" in sql:
            return httpx.Response(200, text=f"{max(self.archive, default=0)}\n")
        return httpx.Response(200, text="")


def clickhouse_at_the_transport(monkeypatch, fake: FakeClickHouse) -> FakeClickHouse:
    """`KS_CH_URL` set, and every httpx client of the test answered by `fake`."""
    monkeypatch.setenv("KS_CH_URL", "http://clickhouse.test:8123")
    transport = httpx.MockTransport(fake.handle)
    real = httpx.AsyncClient

    class _OverTheFake(real):
        def __init__(self, *args, **kwargs):
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", _OverTheFake)
    return fake


@asynccontextmanager
async def kept_mirror_state(*tables: str, dsn: Optional[str] = None) -> AsyncIterator[None]:
    """`meta.mirror_state`'s rows for `tables`: absent for the block, put back
    as they were after it."""
    conn = await asyncpg.connect(dsn or DSN)
    try:
        names = list(tables)
        saved = await conn.fetch(
            "SELECT * FROM meta.mirror_state WHERE table_name = ANY($1::text[])", names)
        await conn.execute(
            "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])", names)
        try:
            yield
        finally:
            await conn.execute(
                "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])", names)
            for row in saved:
                columns = list(row.keys())
                holes = ", ".join(f"${i}" for i in range(1, len(columns) + 1))
                await conn.execute(
                    f"INSERT INTO meta.mirror_state ({', '.join(columns)}) VALUES ({holes})",
                    *row.values())
    finally:
        await conn.close()


async def stamp(pool, table: str, *, last_ok_at: datetime, failures: int = 0) -> None:
    """A state row as a ship leaves it: its last success, and the failures
    counted since (each one attempted after that success)."""
    await pool.execute(
        "INSERT INTO meta.mirror_state (table_name, last_attempted_at, last_ok_at,"
        " failures_since_ok, last_rows) VALUES ($1, now(), $2, $3, 0)",
        table, last_ok_at, failures)


class Archive:
    """`app.order_versions` and the ship's state row, written by a test."""

    def __init__(self, pool):
        self.pool = pool

    async def versions(self, ids: Iterable[int], *, captured_at: datetime) -> None:
        await self.pool.executemany(
            "INSERT INTO app.order_versions (id, order_id, captured_at, kind)"
            " VALUES ($1, $2, $3, 'change')",
            [(i, 900_000 + i, captured_at) for i in ids])

    async def shipped(self, *, last_ok_at: datetime, failures: int = 0) -> None:
        from core.ch_history import STATE_ROW

        await stamp(self.pool, STATE_ROW, last_ok_at=last_ok_at, failures=failures)


@asynccontextmanager
async def archive_in_postgres(monkeypatch) -> AsyncIterator[Archive]:
    """The archive of record empty for the block and after it, the ship's and
    the heal's state rows absent and then put back, `core.pg.get_pool`
    answering the test database, and no heal this process remembers."""
    from core import ch_history

    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.setattr(ch_history, "last_heal", {})
    async with kept_mirror_state(ch_history.STATE_ROW, ch_history.HEAL_ROW):
        pool = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
        try:
            await pool.execute("DELETE FROM app.order_versions")
            with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
                yield Archive(pool)
        finally:
            await pool.execute("DELETE FROM app.order_versions")
            await pool.close()


async def fresh_clickhouse_copies() -> None:
    """Silver's and Gold's copies dropped from the real ClickHouse: the ship
    creates a table only when it is absent, and another test may have left
    one in a shape of its own."""
    from core.ch_common import execute
    from core.ch_silver import GOLD_STAGING, GOLD_TABLE, SILVER_STAGING, SILVER_TABLE

    for database, tables in (("silver", (SILVER_TABLE, SILVER_STAGING)),
                             ("gold", (GOLD_TABLE, GOLD_STAGING))):
        await execute(f"CREATE DATABASE IF NOT EXISTS {database}")
        for table in tables:
            await execute(f"DROP TABLE IF EXISTS {table}")
