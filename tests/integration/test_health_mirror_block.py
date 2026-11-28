"""`/api/health` publishes the age of the Postgres copy of landing.

The hole: a broken `bronze.orders` mirror was silent until the 07:30
comparison. The watchdog that judges this lives in the *other* container
(bot/canary.py), which is the whole point — a container cannot be the witness
to its own silence.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from tests.helpers.app_client import ConnectionPerAcquire
from tests.integration.pg_tab_seed import DSN, needs_postgres
from web.main import app
from web.routes.api import health as health_routes

ORDERS = "bronze.orders"


@pytest.fixture(autouse=True)
def _empty_mirror_cache():
    """The block is cached for 60 s on a process-wide dict."""
    health_routes._mirror_cache["data"] = None
    health_routes._mirror_cache["expires_at"] = 0
    yield
    health_routes._mirror_cache["data"] = None
    health_routes._mirror_cache["expires_at"] = 0


@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    from web.ratelimit import limiter

    limiter.reset()
    yield
    limiter.reset()


@pytest.fixture
def client():
    return TestClient(app)


@pytest_asyncio.fixture
async def orders_watermark():
    """Writes `bronze.orders`' row of meta.mirror_state; whatever stood there
    before is put back after the test."""
    conn = await asyncpg.connect(DSN)
    try:
        kept = await conn.fetchrow(
            "SELECT * FROM meta.mirror_state WHERE table_name = $1", ORDERS)
    finally:
        await conn.close()

    async def write(*, ok_ago_s: int, failures: int, error) -> None:
        now = datetime.now(timezone.utc)
        ok_at = now - timedelta(seconds=ok_ago_s)
        conn = await asyncpg.connect(DSN)
        try:
            await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = $1", ORDERS)
            await conn.execute(
                "INSERT INTO meta.mirror_state (table_name, last_attempted_at, last_ok_at,"
                " failures_since_ok, last_error) VALUES ($1, $2, $3, $4, $5)",
                ORDERS, now - timedelta(seconds=60) if failures else ok_at, ok_at,
                failures, error)
        finally:
            await conn.close()

    yield write
    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = $1", ORDERS)
        if kept is not None:
            columns = list(kept.keys())
            await conn.execute(
                f"INSERT INTO meta.mirror_state ({', '.join(columns)}) VALUES "
                f"({', '.join(f'${i}' for i in range(1, len(columns) + 1))})",
                *kept.values())
    finally:
        await conn.close()


class TestTheBlock:
    @needs_postgres
    @pytest.mark.asyncio
    @pytest.mark.parametrize("ok_ago_s, failures, error, failing", [
        pytest.param(180, 0, None, False, id="shipped three minutes ago"),
        # The canary pages on `failing` at the next sync tick, whatever the
        # age; the driver's text carries a DSN and stays out of the body.
        pytest.param(1800, 2, "InvalidPasswordError: password authentication failed",
                     True, id="two failures since the last shipment"),
    ])
    async def test_it_is_read_from_the_orders_watermark(
            self, orders_watermark, monkeypatch, ok_ago_s, failures, error, failing):
        await orders_watermark(ok_ago_s=ok_ago_s, failures=failures, error=error)
        monkeypatch.setattr(health_routes, "_mirror_cache_lock", asyncio.Lock())

        with patch("core.pg.get_pool", new=AsyncMock(return_value=ConnectionPerAcquire(DSN))):
            response = await asyncio.to_thread(TestClient(app).get, "/api/health")

        assert response.status_code == 200
        entry = response.json()["mirrors"][ORDERS]
        assert ok_ago_s <= entry["age_seconds"] <= ok_ago_s + 60
        assert entry["failures_since_ok"] == failures
        assert entry["failing"] is failing
        if error:
            assert error not in response.text

    def test_unreadable_is_null_and_never_an_empty_object(self, client, monkeypatch):
        """`{}` would read to the canary as 'asked, nothing watched'; null is
        'nobody is watching', which is a failure over there."""
        async def fake(*a, **kw):
            return None

        monkeypatch.setattr(health_routes, "_mirror_freshness", fake)
        body = client.get("/api/health").json()
        assert body["mirrors"] is None

    def test_a_host_with_no_postgres_still_serves_health(self, client, monkeypatch):
        """`core.pg.dsn()` raises rather than guessing a database, so this path
        is taken on every call in any environment without KS_PG_DSN."""
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        response = client.get("/api/health")
        assert response.status_code == 200
        assert response.json()["mirrors"] is None

    def test_a_postgres_failure_costs_the_block_and_nothing_else(
        self, client, monkeypatch,
    ):
        async def boom(*a, **kw):
            raise RuntimeError("connection refused")

        monkeypatch.setattr("core.pg.get_pool", boom)
        response = client.get("/api/health")
        assert response.status_code == 200
        assert response.json()["mirrors"] is None

    def test_the_block_is_cached_rather_than_queried_per_probe(
        self, client, monkeypatch,
    ):
        """UptimeRobot, nginx and Docker all poll this; one Postgres round trip
        a minute is the budget."""
        calls = []

        async def counting(pool, *a, tables=("bronze.orders",), **kw):
            # Every table asked for: since step 14's S14-00 the derived
            # layers and the UTM table are always among them (`own`, and the
            # parse in Postgres, are constants).
            calls.append(1)
            return {table: {
                "last_ok_at": None, "age_seconds": None,
                "failures_since_ok": 0, "failing": False,
            } for table in tables}

        async def fake_pool():
            return object()

        monkeypatch.setattr("core.pg.get_pool", fake_pool)
        monkeypatch.setattr(
            "core.mirror_reconciliation.fetch_mirror_freshness", counting,
        )
        for _ in range(3):
            client.get("/api/health")
        assert len(calls) == 1


class TestTheReader:
    @pytest.mark.asyncio
    async def test_it_reports_age_failures_and_never_the_error_text(self):
        """`/api/health` is public. `last_error` is a driver exception string,
        which on this database carries a DSN; whether there *is* one is the
        part a watchdog acts on."""
        from core.mirror_reconciliation import fetch_mirror_freshness

        now = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
        state = {"bronze.orders": {
            "last_ok_at": now - timedelta(minutes=30),
            "failures_since_ok": 2,
            "last_error": "InvalidPasswordError: password authentication failed",
        }}

        import core.mirror_reconciliation as mr

        async def fake_watermarks(pool):
            return state

        original = mr.fetch_watermarks
        mr.fetch_watermarks = fake_watermarks
        try:
            out = await fetch_mirror_freshness(None, now=now)
        finally:
            mr.fetch_watermarks = original

        entry = out["bronze.orders"]
        assert entry["age_seconds"] == 1800
        assert entry["failures_since_ok"] == 2
        assert entry["failing"] is True
        assert "password" not in str(entry)

    @pytest.mark.asyncio
    async def test_a_table_with_no_row_reports_never_not_forever_ago(self):
        import core.mirror_reconciliation as mr

        async def fake_watermarks(pool):
            return {}

        original = mr.fetch_watermarks
        mr.fetch_watermarks = fake_watermarks
        try:
            out = await mr.fetch_mirror_freshness(None)
        finally:
            mr.fetch_watermarks = original

        entry = out["bronze.orders"]
        assert entry["last_ok_at"] is None
        assert entry["age_seconds"] is None
        assert entry["failures_since_ok"] is None

    def test_clickhouse_rows_are_not_watched(self):
        """Its daily window of silence was accepted when step 4 shipped."""
        from core.mirror_reconciliation import WATCHED_MIRRORS

        assert WATCHED_MIRRORS == ("bronze.orders",)
