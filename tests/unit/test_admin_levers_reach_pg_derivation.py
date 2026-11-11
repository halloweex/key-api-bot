"""Chain 2, step 7: the lever every Silver/Gold alert names reaches Postgres.

Under KS_PG_DERIVE=own, POST /api/warehouse/refresh (and rebuild-silver) run the
Postgres derivation past its floor, after releasing the heavy lock the
derivation takes itself; GET /api/warehouse/status shows the owed state.

Since step 14's S14-00 `own` and step 13's `postgres` are constants, so DuckDB
derives nothing on the lever, the status leads with Postgres, and the tests
of piggyback went with it.
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
def mode():
    """The cached KS_PG_DERIVE mode's setter until step 14's S14-15 removed
    the mode; kept as a fixture that sets nothing, so a test still says it
    was written for `own` — the only state there is."""
    return lambda _value: None


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
    store.connection = MagicMock(side_effect=AssertionError("DuckDB touched"))
    with patch("core.scheduler.get_scheduler", return_value=scheduler), \
         patch.object(admin, "get_store", AsyncMock(return_value=store)):
        return asyncio.run(_unwrap(admin.refresh_warehouse)(MagicMock(), admin={}))


class TestRefresh:
    def test_it_derives_postgres_past_the_floor(self, mode):
        mode("own")
        scheduler = _Scheduler()
        result = _refresh(scheduler)
        assert scheduler.calls == [("manual", True)]
        assert result == {"trigger": "manual",
                          "postgres": {"status": "success", "trigger": "manual"}}


class TestStatus:
    def _status(self):
        from web.routes.api import admin

        block = {"requested": 3, "built": 3, "owed": False}
        with patch.object(admin, "_pg_derivation_status", AsyncMock(return_value=block)):
            return asyncio.run(_unwrap(admin.get_warehouse_status)(MagicMock()))

    def test_it_shows_the_owed_state_and_nothing_of_step_13(self, mode):
        """`duckdb_frozen` and `cutover` (step 13's readiness) went in step
        14's S14-15 with `core/warehouse_cutover.py`."""
        mode("own")
        assert self._status() == {"writer": "postgres",
                                  "postgres": {"requested": 3, "built": 3, "owed": False}}
