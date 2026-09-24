"""Chain 2, step 7: the lever every Silver/Gold alert names reaches Postgres.

Under KS_PG_DERIVE=own, POST /api/warehouse/refresh (and rebuild-silver) run the
Postgres derivation past its floor, after releasing the heavy lock the
derivation takes itself; GET /api/warehouse/status shows the owed state. Under
piggyback, nothing about the three routes changes.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core import pg_derivation


@pytest.fixture
def mode(monkeypatch):
    before = (pg_derivation._mode, pg_derivation._mode_error)

    def set_mode(value):
        if value is None:
            monkeypatch.delenv(pg_derivation.ENV, raising=False)
        else:
            monkeypatch.setenv(pg_derivation.ENV, value)
        pg_derivation.configure_mode()

    yield set_mode
    pg_derivation._mode, pg_derivation._mode_error = before


def _unwrap(fn):
    while hasattr(fn, "__wrapped__"):
        fn = fn.__wrapped__
    return fn


class _Scheduler:
    def __init__(self):
        self._heavy_job_lock = asyncio.Lock()
        self.calls = []

    async def _run_pg_derivation(self, *, trigger=None, force=False):
        # The derivation takes the heavy lock itself; called under it, it
        # would wait for ever.
        assert not self._heavy_job_lock.locked(), "called while holding the heavy lock"
        self.calls.append((trigger, force))
        return {"status": "success", "trigger": trigger}


def _refresh(scheduler):
    from web.routes.api import admin

    store = MagicMock()
    store.refresh_warehouse_layers = AsyncMock(return_value={"status": "success"})
    with patch("core.scheduler.get_scheduler", return_value=scheduler), \
         patch.object(admin, "get_store", AsyncMock(return_value=store)):
        return asyncio.run(_unwrap(admin.refresh_warehouse)(MagicMock(), admin={}))


class TestRefresh:
    def test_under_own_it_derives_postgres_past_the_floor(self, mode):
        mode("own")
        scheduler = _Scheduler()
        result = _refresh(scheduler)
        assert scheduler.calls == [("manual", True)]
        assert result["status"] == "success"
        assert result["postgres"]["trigger"] == "manual"

    def test_under_piggyback_it_is_unchanged(self, mode):
        mode(None)
        scheduler = _Scheduler()
        result = _refresh(scheduler)
        assert scheduler.calls == []
        assert result == {"status": "success"}


CUTOVER = {"mode": "duckdb", "preconditions_met": False,
           "unmet": [{"key": "pg_derive_own", "detail": "..."}]}


class TestStatus:
    def _status(self, readiness=None):
        from core import warehouse_cutover
        from web.routes.api import admin

        store = MagicMock()
        store.get_warehouse_status = AsyncMock(return_value={"last_refresh": "x"})
        block = {"mode": "own", "owed": False}
        readiness = readiness or AsyncMock(return_value=CUTOVER)
        with patch.object(admin, "get_store", AsyncMock(return_value=store)), \
             patch.object(admin, "_pg_derivation_status", AsyncMock(return_value=block)), \
             patch.object(warehouse_cutover, "readiness", readiness):
            return asyncio.run(_unwrap(admin.get_warehouse_status)(MagicMock()))

    def test_under_own_it_shows_the_owed_state(self, mode):
        mode("own")
        assert self._status() == {"last_refresh": "x",
                                  "postgres": {"mode": "own", "owed": False},
                                  "cutover": CUTOVER}

    def test_under_piggyback_it_adds_only_the_readiness(self, mode):
        mode(None)
        assert self._status() == {"last_refresh": "x", "cutover": CUTOVER}


class TestTheCutoverReadiness:
    """DN-28: step 13's readiness on the status page an admin already reads
    before a warehouse lever — the mode as read, and every unmet precondition
    by name. Nothing is switched."""

    def test_it_is_the_evaluators_answer(self, mode):
        mode(None)
        from core import warehouse_cutover

        with patch("core.pg.current_revision", AsyncMock(return_value=None)):
            body = TestStatus()._status(readiness=warehouse_cutover.readiness)
        cutover = body["cutover"]
        assert cutover["variable"] == "KS_WRITE_WAREHOUSE"
        assert cutover["mode"] == "duckdb" and cutover["switch_built"] is False
        keys = [u["key"] for u in cutover["unmet"]]
        assert keys and set(keys) <= set(cutover["preconditions"])

    def test_a_readiness_that_raises_costs_the_block_not_the_page(self, mode, caplog):
        """By its class: a driver's text names the database user, host and
        port. The whole of it goes to the log."""
        mode(None)
        text = 'password authentication failed for user "ks_app"'
        with caplog.at_level("ERROR", logger="web.routes.api.admin"):
            body = TestStatus()._status(readiness=AsyncMock(side_effect=RuntimeError(text)))
        assert body["last_refresh"] == "x"
        assert body["cutover"]["readiness_error"] == "RuntimeError"
        assert "ks_app" not in repr(body)
        assert text in caplog.text
        assert body["cutover"]["mode"] == "duckdb"
