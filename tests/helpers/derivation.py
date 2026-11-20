"""The Postgres derivation's job, driven step by step over a fake pool."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_derivation


@pytest.fixture
def mode():
    """The cached mode's setter until step 14's S14-15 removed the mode;
    kept as a fixture that sets nothing, so a test still says it was written
    for `own` — the only state there is."""
    return lambda _value: None


class _Pool:
    """A pool whose connection is never really used: every read and write the
    job makes through it is patched out below."""

    def acquire(self):
        class _Ctx:
            async def __aenter__(self):
                return object()

            async def __aexit__(self, *exc):
                return False

        return _Ctx()


def _verdict(passed: bool = True) -> dict:
    return {"passed": passed, "row_count_match": passed, "checksum_match": True,
            "cells_match": True, "rollup_match": True, "partition_exhaustive": True,
            "missing_cells": 0, "extra_cells": 0, "rollup_mismatch_cells": 0,
            "unknown_sales_types": []}


def _bounded(coro, timeout=10):
    """Every run takes `_heavy_job_lock` and PG_LAYER_LOCK; a mutation that
    left one held must fail the test, not hang the suite."""
    return asyncio.run(asyncio.wait_for(coro, timeout=timeout))


@pytest.fixture
def job(mode, monkeypatch):
    """A scheduler under own whose process has already derived once, so a tick
    with nothing owed is quiet — a web container past its first minute. Every
    piece of process state the job keeps is put back afterwards."""
    from core.scheduler import BackgroundScheduler

    mode("own")
    monkeypatch.delenv("KS_PG_SILVER_INTERVAL_S", raising=False)   # the 600 s floor
    before = (BackgroundScheduler._pg_derive_ran, BackgroundScheduler._pg_derive_started,
              pg_derivation._signal_failures)
    BackgroundScheduler._pg_derive_ran = True
    BackgroundScheduler._pg_derive_started = None
    pg_derivation._signal_failures = 0
    yield BackgroundScheduler()
    (BackgroundScheduler._pg_derive_ran, BackgroundScheduler._pg_derive_started,
     pg_derivation._signal_failures) = before


_STEPS = ("silver", "gold", "validate", "complete", "record_run")


def _derive(job, *, reads=((3, 2, None),), journal_attempt=None, verdict=None,
            raising=(), utm=None):
    """One tick through `_run_pg_derivation` with every Postgres step patched.
    `raising` names the steps that raise; `reads` are what successive signal
    reads return or raise. Returns the result and the mocks."""
    values = {"silver": {"rows": 1}, "gold": {"rows": 1, "cells": 1},
              "validate": verdict or _verdict(),
              "complete": None, "record_run": 1}
    mocks = {name: (AsyncMock(side_effect=RuntimeError(f"{name} boom")) if name in raising
                    else AsyncMock(return_value=values[name])) for name in _STEPS}
    mocks["record_failure"] = AsyncMock()
    mocks["clear_tail"] = AsyncMock(return_value=False)
    # The derivation's last step since S14-00's constant (`KS_UTM_PARSE`
    # postgres): its own failure is its own, recorded by the parse, never
    # the derivation's — so here it simply succeeds, unless a test hands its
    # own (`tests/unit/test_utm_parse_wiring.py`).
    mocks["utm"] = utm if utm is not None else AsyncMock(return_value={"status": "success"})
    with patch("core.pg.get_pool", AsyncMock(return_value=_Pool())), \
         patch("core.pg.require_revision", AsyncMock()), \
         patch("core.pg_utm_parse.parse_incremental_locked", mocks["utm"]), \
         patch("core.pg_silver.rebuild_silver", mocks["silver"]), \
         patch("core.pg_gold.rebuild_gold", mocks["gold"]), \
         patch("core.pg_landing._record_failure", mocks["record_failure"]), \
         patch.object(pg_derivation, "read_owed", AsyncMock(side_effect=list(reads) * 2)), \
         patch.object(pg_derivation, "last_attempt_at", AsyncMock(return_value=journal_attempt)), \
         patch.object(pg_derivation, "validate", mocks["validate"]), \
         patch.object(pg_derivation, "complete", mocks["complete"]), \
         patch.object(pg_derivation, "record_run", mocks["record_run"]), \
         patch.object(pg_derivation, "heal_dropped_marks", AsyncMock(return_value=False)), \
         patch.object(pg_derivation, "clear_tail_failures", mocks["clear_tail"]), \
         patch("core.alerting.raise_alert", AsyncMock(return_value=1)), \
         patch("core.alerting.resolve_group", AsyncMock(return_value=0)):
        return _bounded(job._run_pg_derivation()), mocks
