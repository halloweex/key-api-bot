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


class TestStatus:
    def _status(self):
        from web.routes.api import admin

        store = MagicMock()
        store.get_warehouse_status = AsyncMock(return_value={"last_refresh": "x"})
        block = {"mode": "own", "owed": False}
        with patch.object(admin, "get_store", AsyncMock(return_value=store)), \
             patch.object(admin, "_pg_derivation_status", AsyncMock(return_value=block)):
            return asyncio.run(_unwrap(admin.get_warehouse_status)(MagicMock()))

    def test_under_own_it_shows_the_owed_state(self, mode):
        mode("own")
        assert self._status() == {"last_refresh": "x", "postgres": {"mode": "own", "owed": False}}

    def test_under_piggyback_it_is_unchanged(self, mode):
        mode(None)
        assert self._status() == {"last_refresh": "x"}
