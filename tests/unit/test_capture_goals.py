"""Revision 0036's goal capture, without a database: who writes it, and what
each write path says it was.

`app.revenue_goals` keeps one row per period (`ON CONFLICT (period_type)`), so
every goal ever set lives only in `app.revenue_goal_history`, which chain 7a's
writer appends inside the goal's own transaction
(`core/pg_goals_write.py`, "AND EVERY GOAL IS KEPT"). What that has to hold
here:

- every function that writes the goal table writes its history, and nothing
  else writes either — so no path to a goal can skip the capture;
- the two routes that write a goal name what they did: `POST /api/goals` a
  custom goal (`set`, derived from `is_custom`), `DELETE /api/goals/{period}`
  a reset (`reset_to_auto`, said by the repository);
- a kind the history's readers do not know is refused before the latch, like
  every other argument the writer refuses.

`tests/integration/test_capture_goals_pg.py` proves the rest on a server.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from core import pg_goals_write
from core.repositories.goals import GoalsMixin
from tests.unit.test_read_fallback_sites import admin_client  # noqa: F401 — fixture
from tests.unit.test_write_chains import _CAPTURE_WALKED, _Walk

GOALS = "app.revenue_goals"
HISTORY = "app.revenue_goal_history"


@pytest.fixture(scope="module")
def walk():
    return _Walk(_CAPTURE_WALKED)


def _writing(walk, table) -> set:
    return {key for key, targets in walk.writers().items() if table in targets}


class TestNoGoalIsWrittenWithoutItsVersion:
    def test_the_goal_and_its_history_have_one_writer_and_it_is_the_same(self, walk):
        """Kills: a second writer of the goal table (the Monday job storing a
        recalculated goal, say) that does not append a version — the history
        would then miss exactly the goals nobody typed — and the capture
        moved out of `set_goal` into a function of its own."""
        writer = {("core/pg_goals_write.py", "set_goal")}
        assert _writing(walk, GOALS) == writer
        assert _writing(walk, HISTORY) == writer

    def test_the_kinds_are_the_columns_comment(self):
        """Revision 0036 documents `change_kind` as `set | recalculated |
        reset_to_auto`, and the warehouse's `goal_history` the same. Parsed
        from the revision's own statement, so a kind added on one side
        fails here."""
        import re

        from tests import migration_replay as replay

        rev = replay.load(replay.VERSIONS / "0036_analytics_captures.py")
        ddl = next(s for s in rev.STATEMENTS if HISTORY in s)
        documented = re.search(r"change_kind\s+TEXT\s+NOT NULL,\s*--\s*(.+)", ddl).group(1)
        assert tuple(k.strip() for k in documented.split("|")) == pg_goals_write.CHANGE_KINDS


class _Store(GoalsMixin):
    """The repository's two goal writers, with the suggestion fixed: what the
    routes reach, minus the revenue history."""

    def __init__(self):
        self.calculate_suggested_goals = AsyncMock(return_value={
            p: {"suggested": 50_000.0} for p in pg_goals_write.PERIOD_TYPES})


@pytest.fixture
def writer(monkeypatch):
    """The routes' real store down to chain 7a's writer, which records."""
    store = _Store()
    monkeypatch.setattr("web.routes.api.goals.get_store", AsyncMock(return_value=store))
    recorded = AsyncMock(return_value=())
    monkeypatch.setattr(pg_goals_write, "set_goal", recorded)
    return recorded


def _call(recorded):
    """`(is_custom, change_kind)` of the one write, however it was passed."""
    recorded.assert_awaited_once()
    args = recorded.await_args.args
    return args[2], args[6]


class TestTheRoutesSayWhatTheyWrote:
    def test_a_typed_goal_is_a_custom_goal_with_its_kind_derived(
            self, admin_client, writer):  # noqa: F811
        response = admin_client.post("/api/goals?period_type=monthly&amount=4000000")
        assert response.status_code == 200, response.text
        # None is "derive it": `set` for a custom goal (the writer's rule, run
        # on a server in the integration tests).
        assert _call(writer) == (True, None)

    def test_a_reset_says_reset_to_auto(self, admin_client, writer):  # noqa: F811
        response = admin_client.delete("/api/goals/daily")
        assert response.status_code == 200, response.text
        assert _call(writer) == (False, "reset_to_auto")


class TestAnUnknownKindIsRefusedBeforeTheLatch:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("kind", ["edited", "SET", "reset"])
    async def test_nothing_is_reached(self, monkeypatch, kind):
        """The latch is permanent (`_fits`' reason): an argument the writer
        refuses must be refused before it, and before the pool."""
        pool = AsyncMock()
        latch = MagicMock()
        monkeypatch.setattr(pg_goals_write, "_pool", pool)
        monkeypatch.setattr(pg_goals_write, "_latch", latch)

        with pytest.raises(ValueError, match="change_kind"):
            await pg_goals_write.set_goal(
                "monthly", 1_000_000.0, True, 900_000.0, 1.1,
                datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc), kind)

        pool.assert_not_awaited()
        latch.assert_not_called()
