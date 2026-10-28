"""`/api/health` says how long ago the sync last ran.

The block was computed under an import of a name `core.config` has never
defined — `DEFAULT_TZ` lives in `core.sync_service` and
`core.duckdb_constants` — inside an `except Exception` that logs at DEBUG.
So from 2026-02-12, whenever a sync had run at all, the ImportError replaced
the block with `null`: `status`, `seconds_since_sync`, the backoff and the
off-hours flag never reached the endpoint `HealthResponse` documents them
on, and the canary, which reads `sync.seconds_since_sync`, always read None.
Found by the type checker, not by a test.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest


class _Service:
    def __init__(self, ago_s):
        self._last = datetime.now(timezone.utc) - timedelta(seconds=ago_s)

    def get_sync_stats(self):
        return {
            "last_sync_time": self._last.isoformat(),
            "consecutive_empty_syncs": 2,
            "current_backoff_seconds": 60,
            "is_off_hours": False,
        }


@pytest.mark.parametrize("ago_s, status", [(120, "active"), (1800, "stale")])
def test_the_sync_block_is_published(monkeypatch, ago_s, status):
    """Mutation: import `DEFAULT_TZ` from `core.config` again — `sync: null`."""
    from fastapi.testclient import TestClient

    from core import duckdb_store, sync_service
    from web.main import app
    from web.ratelimit import limiter

    monkeypatch.setattr(sync_service, "get_sync_service",
                        AsyncMock(return_value=_Service(ago_s)))
    limiter.reset()
    try:
        body = TestClient(app).get("/api/health").json()
    finally:
        limiter.reset()
        asyncio.run(duckdb_store.close_store())

    sync = body["sync"]
    assert sync is not None, "the sync block was dropped"
    assert sync["status"] == status
    assert ago_s - 5 <= sync["seconds_since_sync"] <= ago_s + 60
    assert sync["consecutive_empty_syncs"] == 2
