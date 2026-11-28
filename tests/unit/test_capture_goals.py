"""Revision 0036's goal capture, without a database: who writes it, and what
each write path says it was.

`app.revenue_goals` keeps one row per period (`ON CONFLICT (period_type)`), so
every goal ever set lives only in `app.revenue_goal_history`, which chain 7a's
writer appends inside the goal's own transaction
(`core/pg_goals_write.py`, "AND EVERY GOAL IS KEPT"). What that has to hold
here:

- every function that writes the goal table writes its history, and nothing
  else writes either — so no path to a goal can skip the capture;
- a kind the history's readers do not know is refused before the latch, like
  every other argument the writer refuses.

`tests/integration/test_capture_goals_pg.py` proves the rest on a server,
the two routes' kinds (`set`, `reset_to_auto`) included.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from core import pg_goals_write
from tests.helpers.write_walk import _CAPTURE_WALKED, _Walk

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


class TestAnUnknownKindIsRefusedBeforeTheLatch:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("kind", ["edited", "SET", "reset"])
    async def test_nothing_is_reached(self, monkeypatch, tmp_path, kind):
        """The latch is permanent (`_fits`' reason): an argument the writer
        refuses must be refused before it, and before the database. Kills:
        the kind checked after the marker is written, or after a connection
        is asked for."""
        from core import chain_latch

        markers = tmp_path / "write-chain-owners"
        monkeypatch.setattr(chain_latch, "MARKER_DIR", markers)

        async def no_database():
            raise AssertionError("the writer asked for a connection")

        monkeypatch.setattr("core.pg.get_pool", no_database)

        with pytest.raises(ValueError, match="change_kind"):
            await pg_goals_write.set_goal(
                "monthly", 1_000_000.0, True, 900_000.0, 1.1,
                datetime(2026, 11, 13, 9, 0, tzinfo=timezone.utc), kind)

        assert not markers.exists() or not any(markers.iterdir())
