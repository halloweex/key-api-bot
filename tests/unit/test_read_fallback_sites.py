"""Two handlers beside the routers: what each lets through.

Every router reads one engine and refuses its failure through
`core.read_fallback.refuse`, which counts the refusal and hands back the
`ReadUnavailable` an HTTP route answers 503 naming the surface; that every
route does so, read off the app, is `tests/unit/test_read_fallback_http.py`'s
sweep (`TestEveryRoute`), which replaced the walk of `core/` and `web/` that
stood here until 2026-11. What is left here is two places that answer
something else on purpose:

- `pg_expenses_read.backfilled()` answers "no history yet" without counting
  a refusal: that is routing, and the read behind it is what refuses
  (`tests/unit/test_pg_expenses_read.py`, over Postgres);
- the smart goal's forecast signal leaves out a signal it cannot read, but
  lets a refusal through rather than reading it as a missing signal.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core import read_fallback


# ─── backfilled(): the site nobody's grep found ────────────────────────────

class TestBackfilledCounts:
    @pytest.fixture(autouse=True)
    def _fresh(self, fresh_read_fallback):
        from core import pg_expenses_read as r

        before = (r._backfilled, r._checked_at, r._check_failed)
        r._backfilled, r._checked_at, r._check_failed = False, 0.0, None
        yield r
        r._backfilled, r._checked_at, r._check_failed = before


    @pytest.mark.asyncio
    async def test_no_history_yet_is_routing_not_a_fallback(self, _fresh):
        conn = MagicMock()
        conn.fetchval = AsyncMock(return_value=False)
        acquire = MagicMock()
        acquire.__aenter__ = AsyncMock(return_value=conn)
        acquire.__aexit__ = AsyncMock(return_value=False)
        pool = MagicMock()
        pool.acquire = MagicMock(return_value=acquire)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            assert await _fresh.backfilled() is False
            assert await _fresh.backfilled() is False
        assert "expenses" not in read_fallback.refusals()


# ─── The one handler around a router: ReadUnavailable passes through ───────

class TestTheForecastSignalLetsARefusalThrough:
    @pytest.mark.asyncio
    async def test_read_unavailable_is_not_a_missing_signal(self):
        from core.repositories.goals import GoalsMixin

        store = GoalsMixin.__new__(GoalsMixin)
        with patch.object(GoalsMixin, "_goals_run",
                          new=AsyncMock(side_effect=read_fallback.ReadUnavailable("goals"))), \
             patch.object(GoalsMixin, "_forecast_actual_revenue",
                          new=AsyncMock(return_value=0.0)):
            with pytest.raises(read_fallback.ReadUnavailable):
                await store._get_ml_forecast_total(2025, 3, "retail")

    @pytest.mark.asyncio
    async def test_any_other_failure_is_still_left_out(self):
        from core.repositories.goals import GoalsMixin

        store = GoalsMixin.__new__(GoalsMixin)
        with patch.object(GoalsMixin, "_goals_run",
                          new=AsyncMock(side_effect=RuntimeError("duckdb gone"))), \
             patch.object(GoalsMixin, "_forecast_actual_revenue",
                          new=AsyncMock(return_value=0.0)):
            assert await store._get_ml_forecast_total(2025, 3, "retail") == 0.0
