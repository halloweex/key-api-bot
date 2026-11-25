"""The admin's full buyer sync and the manual one, and the heavy-job lock.

The full sync writes every buyer KeyCRM lists in portions, each under the
heavy-job lock, and counts the buyers Postgres holds before and after; it
runs here against the test Postgres with KeyCRM faked at its HTTP
transport. The routes answer at once and refuse a second run, and the
manual sync gives up on a busy lock inside the request budget.

Since step 14's S14-00 chain 4 writes the buyers, a constant, so DuckDB's
portions and their mirroring are no path production takes: those tests, and
the refusal of a flag nobody can read, went with it. Since S14-13
`mirror_buyers` itself is gone (`core.pg_buyers`), and with it the walk that
held it to no caller.
"""
from __future__ import annotations

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.helpers.buyers_fixture import (
    buyer_payload, buyers_in_postgres, keycrm_serving_buyers)
from tests.integration.pg_tab_seed import needs_postgres


class _FakeLock:
    """A heavy-job lock that is always taken: acquire waits forever."""

    def locked(self):
        return True

    async def acquire(self):
        await asyncio.sleep(3600)

    def release(self):  # pragma: no cover — never acquired
        raise AssertionError("released a lock it never held")


def _client():
    from fastapi.testclient import TestClient

    from core.permissions import ADMIN_USER_IDS
    from web.main import app
    from web.routes.auth import SESSION_COOKIE, create_session_data, session_serializer

    cookie = session_serializer.dumps(create_session_data({
        "id": str(sorted(ADMIN_USER_IDS)[0]), "first_name": "T", "last_name": "U",
        "username": "t", "auth_date": str(int(time.time()))}, role="admin"))
    client = TestClient(app)
    client.cookies.set(SESSION_COOKIE, cookie)
    return client


@pytest.fixture
def fresh_limits():
    from web.routes.api._deps import limiter
    limiter.reset()
    yield
    limiter.reset()


@needs_postgres
class TestTheFullSync:
    @pytest.mark.asyncio
    async def test_every_buyer_keycrm_lists_lands_and_the_new_ones_are_counted(
            self, monkeypatch):
        """Two buyers held, KeyCRM listing four over two pages — one of the
        two renamed, three new — written in two portions: every buyer lands
        with its phone, and the counts are Postgres's before and after."""
        from core.store import AnalyticsStore
        from web.routes.api import admin

        async with buyers_in_postgres() as pool:
            await pool.executemany(
                "INSERT INTO bronze.buyers (id, full_name) VALUES ($1, $2)",
                [(1, "Олена Петренко"), (2, "Іван Коваль")])
            keycrm_serving_buyers(monkeypatch, [
                [buyer_payload(2, "Іван Коваленко"), buyer_payload(3, "Марія Шевченко")],
                [buyer_payload(4, "Петро Бондар"), buyer_payload(5, "Оксана Ткаченко")],
            ])
            store = AnalyticsStore()
            store.BUYER_WRITE_PORTION = 3       # three buyers, then one

            result = await admin._sync_all_buyers(store)

            names = {r["id"]: r["full_name"] for r in await pool.fetch(
                "SELECT id, full_name FROM bronze.buyers")}
            with_phone = {r["buyer_id"] for r in await pool.fetch(
                "SELECT buyer_id FROM bronze.buyer_contacts WHERE contact_type = 'phone'")}
        assert result == {"status": "done", "buyers_fetched": 4, "buyers_written": 4,
                          "before_count": 2, "after_count": 5, "new_buyers": 3}
        assert names == {1: "Олена Петренко", 2: "Іван Коваленко", 3: "Марія Шевченко",
                         4: "Петро Бондар", 5: "Оксана Ткаченко"}
        assert with_phone == {2, 3, 4, 5}


class TestTheRoutes:
    def test_sync_all_answers_at_once_and_runs_detached(self, monkeypatch, fresh_limits):
        from web.routes.api import admin

        body = AsyncMock(return_value={"status": "done"})
        monkeypatch.setattr(admin, "_sync_all_buyers", body)
        monkeypatch.setattr(admin, "get_store", AsyncMock(return_value=MagicMock()))

        r = _client().post("/api/sync/sync-all-buyers")

        assert r.status_code == 200, r.text
        assert r.json()["status"] == "started"

    def test_a_second_full_sync_while_one_runs_is_refused(self, monkeypatch, fresh_limits):
        from web.routes.api import admin

        running = MagicMock()
        running.get_name.return_value = "sync_all_buyers"
        running.done.return_value = False
        monkeypatch.setattr(admin, "_BACKGROUND_TASKS", {running})

        r = _client().post("/api/sync/sync-all-buyers")

        assert r.status_code == 409, r.text

    def test_the_manual_sync_waits_for_the_same_lock(self, monkeypatch, fresh_limits):
        from core.scheduler import get_scheduler
        from web.routes.api import admin

        monkeypatch.setattr(admin, "HEAVY_LOCK_WAIT_S", 0.05)
        monkeypatch.setattr(get_scheduler(), "_heavy_job_lock", _FakeLock())

        r = _client().post("/api/sync/sync-buyers")

        assert r.status_code == 409, r.text

    def test_the_manual_syncs_409_fits_inside_the_request_budget(self):
        """At 60 s the app's own 30 s RequestTimeoutMiddleware answered 504
        first, and the handler kept writing behind that answer."""
        from web import middleware
        from web.routes.api import admin

        assert "/api/sync/sync-buyers" not in middleware.SLOW_ENDPOINTS
        assert admin.HEAVY_LOCK_WAIT_S < middleware.DEFAULT_REQUEST_TIMEOUT
