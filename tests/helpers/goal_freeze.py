"""The goal freeze's ledger fixture and the busy hours the job keeps clear."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from core import goal_freeze


# A validated derivation after every moment these tests freeze at, unless a
# test says when the last one started.
DERIVED_LATER = datetime(2100, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def ledger(monkeypatch):
    """The writer's Postgres steps replaced: `held` is what the month holds
    already, `derived_at` when the last validated derivation started, and
    `kept` records what a run would write."""
    monkeypatch.setenv("KS_PG_DSN", "postgresql://nobody@127.0.0.1:1/none")
    state = {"held": set(), "kept": [], "pool": object(), "derived_at": DERIVED_LATER,
             "derivation_read": 0}

    async def _pool():
        return state["pool"]

    async def _held(pool, year, month):
        assert pool is state["pool"]
        return set(state["held"])

    async def _derived_at(pool):
        assert pool is state["pool"]
        state["derivation_read"] += 1
        return state["derived_at"]

    async def _keep(pool, year, month, answers, at):
        state["kept"].append((year, month, list(answers), at))
        return [sales_type for sales_type, _ in answers]

    monkeypatch.setattr(goal_freeze, "_pool", _pool)
    monkeypatch.setattr(goal_freeze, "_held", _held)
    monkeypatch.setattr(goal_freeze, "_derived_at", _derived_at)
    monkeypatch.setattr(goal_freeze, "_keep", _keep)
    return state


#: Minutes of the Kyiv day the job must not start in: 05:00-05:50, 07:20-07:45,
#: 08:55-09:55, ten minutes either side of the 01/07/13/19 checks and of the
#: 02:40 GA4 capture, and the 03:00-04:59 hours a clock change can skip or
#: repeat (the GA4 capture's list, `tests/unit/test_capture_ga4.py`).
BUSY = [(300, 350), (440, 465), (535, 595), (180, 299), (150, 170)] + [
    (h * 60 - 10, h * 60 + 10) for h in (1, 7, 13, 19)]
