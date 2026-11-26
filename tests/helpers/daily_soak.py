"""The daily soak's SQL checks run against the test Postgres: a scenario
seeded, a check rendered and its verdict read.
"""
from __future__ import annotations

import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio


asyncpg = pytest.importorskip("asyncpg")


DSN = os.getenv("KS_PG_DSN")


needs_pg = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")


REPO = Path(__file__).resolve().parents[2]


SQL_DIR = REPO / "deploy" / "daily_soak"


# B3's floor, the one variable a check reads. P1's five — the DuckDB file's
# hash record — went with P1 at the final stage (2026-11-12).
VARIABLES = {"buyers_override_floor": ""}


KYIV = ZoneInfo("Europe/Kyiv")


# A Wednesday, noon in Kyiv: inside no heavy-lock window, after the 07:30 run.
NOW = datetime(2030, 6, 5, 12, 0, tzinfo=KYIV)


def _uncommented(sql: str) -> str:
    return re.sub(r"--[^\n]*", "", sql)


def _variables(sql: str) -> set[str]:
    """psql's quoted interpolations, `:'name'`, outside comments."""
    return set(re.findall(r":'(\w+)'", _uncommented(sql)))


def render(name: str, **variables: str) -> str:
    """The file as `psql -v name=value` would hand it to the server."""
    sql = (SQL_DIR / name).read_text(encoding="utf-8")
    unknown = _variables(sql) - set(VARIABLES)
    assert not unknown, f"{name} uses undocumented psql variables {sorted(unknown)}"
    for key, value in {**VARIABLES, **variables}.items():
        sql = sql.replace(f":'{key}'", "'" + value.replace("'", "''") + "'")
    return sql


@pytest_asyncio.fixture
async def pool():
    p = await asyncpg.create_pool(DSN, min_size=1, max_size=2)
    yield p
    await p.close()


@asynccontextmanager
async def scenario(pool):
    """One transaction, always rolled back."""
    async with pool.acquire() as conn:
        tr = conn.transaction()
        await tr.start()
        try:
            yield conn
        finally:
            await tr.rollback()


async def check(conn, name: str, *, now: datetime | None = NOW, **variables: str):
    if now is not None:
        await conn.execute("SELECT set_config('soak.now', $1, true)", now.isoformat())
    await conn.execute("SET TRANSACTION READ ONLY")
    rows = await conn.fetch(render(name, **variables))
    assert rows, f"{name} returned no row"
    return [dict(r) for r in rows]


async def verdict(conn, name: str, **kw) -> tuple[str, str]:
    rows = await check(conn, name, **kw)
    assert len(rows) == 1, rows
    return rows[0]["verdict"], rows[0]["detail"]


async def signal(conn, *, requested, built, requested_at, built_at):
    await conn.execute(
        """
        INSERT INTO meta.derivation_signal (layer, requested, built, requested_at, built_at)
        VALUES ('warehouse', $1, $2, $3, $4)
        ON CONFLICT (layer) DO UPDATE SET requested = EXCLUDED.requested,
            built = EXCLUDED.built, requested_at = EXCLUDED.requested_at,
            built_at = EXCLUDED.built_at
        """, requested, built, requested_at, built_at)


async def no_runs(conn):
    await conn.execute("DELETE FROM meta.derivation_runs")


async def run(conn, *, trigger, started_at, seen=7, error=None, passed=True, seconds=3):
    """One journal row. Insert in chronological order: the checks order by id."""
    await conn.execute(
        """
        INSERT INTO meta.derivation_runs
               (layer, trigger, started_at, ended_at, requested_seen,
                validation_passed, validation, error)
        VALUES ('warehouse', $1, $2, $3, $4, $5, $6::jsonb, $7)
        """,
        trigger, started_at, started_at + timedelta(seconds=seconds), seen,
        None if error else passed,
        None if error else '{"bronze_orders": 10, "row_count_match": true}',
        error)
