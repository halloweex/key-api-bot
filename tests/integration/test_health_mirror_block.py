"""`/api/health` publishes the age of the Postgres copy of landing.

The hole: a broken `bronze.orders` mirror was silent until the 07:30
comparison. The watchdog that judges this lives in the *other* container
(bot/canary.py), which is the whole point — a container cannot be the witness
to its own silence.
"""
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from web.main import app
from web.routes.api import health as health_routes


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


class TestTheBlock:
    def test_it_is_published_when_postgres_answers(self, client, monkeypatch):
        async def fake(*a, **kw):
            return {"bronze.orders": {
                "last_ok_at": "2026-08-29T06:32:12+00:00",
                "age_seconds": 180, "failures_since_ok": 0, "failing": False,
            }}

        monkeypatch.setattr(health_routes, "_mirror_freshness", fake)
        body = client.get("/api/health").json()
        assert body["mirrors"]["bronze.orders"]["age_seconds"] == 180
        assert body["mirrors"]["bronze.orders"]["failing"] is False

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

        async def counting(pool, *a, **kw):
            calls.append(1)
            return {"bronze.orders": {
                "last_ok_at": None, "age_seconds": None,
                "failures_since_ok": 0, "failing": False,
            }}

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
