"""Step 05: the one loud repeat for what nobody resolved or acknowledged.

Judged in the bot from the archive — the process that already watches from
outside — with the review's two storm-guards pinned: a dead web stands the
escalator down (the outage is its own page), and one escalation per firing
cycle, never a stream.
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from core.alert_escalator import (
    ESCALATE_AFTER_S,
    DueCondition,
    escalate_due,
    format_escalation,
)


def _due(key="mirror_failing", hours=7, fired=3):
    return DueCondition(
        condition_key=key, fired_count=fired,
        cycle_start=datetime.now(timezone.utc) - timedelta(hours=hours),
    )


class TestPolicy:
    def test_six_hours_is_the_bar(self):
        """The gap the Gate leaves open: loud hour over, daily reminder 23h
        away. Pinned so a later tuning is a decision, not drift."""
        assert ESCALATE_AFTER_S == 6 * 3600

    @pytest.mark.asyncio
    async def test_a_dead_web_stands_the_escalator_down(self):
        """During an outage every series is stale together and the canary is
        already paging the outage itself — the review's predicted storm."""
        with patch("bot.main.send_admin_message", new=AsyncMock()) as send:
            assert await escalate_due(web_alive=False, _due=[_due()]) == 0
        send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_nothing_due_sends_nothing(self, monkeypatch):
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@y/z")
        with patch("bot.main.send_admin_message", new=AsyncMock()) as send:
            assert await escalate_due(web_alive=True, _due=[]) == 0
        send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_dsn_is_configuration_not_failure(self, monkeypatch):
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        with patch("bot.main.send_admin_message", new=AsyncMock()) as send:
            assert await escalate_due(web_alive=True, _due=[_due()]) == 0
        send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_due_conditions_escalate_in_one_message(self, monkeypatch):
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@y/z")
        rows = [_due("mirror_failing", 7, 3), _due("disk:CRITICAL", 9, 2)]
        with patch("bot.main.send_admin_message",
                   new=AsyncMock(return_value=2)) as send, \
             patch("core.alert_archive.record_escalated") as rec:
            assert await escalate_due(web_alive=True, _due=rows) == 2
        text = send.await_args.args[0]
        assert "mirror_failing — 7ч, срабатываний ×3" in text
        assert "disk:CRITICAL — 9ч" in text
        assert send.await_args.kwargs["pre_throttled"] is True
        assert sorted(rec.call_args.args[0]) == [
            "disk:CRITICAL", "mirror_failing"]

    @pytest.mark.asyncio
    async def test_recorded_even_when_delivery_fails(self, monkeypatch):
        """The escalated event is what stops the next 15-minute tick from
        re-judging the same cycle — a kill-switched dev instance must not
        retry forever."""
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@y/z")
        with patch("bot.main.send_admin_message",
                   new=AsyncMock(return_value=0)), \
             patch("core.alert_archive.record_escalated") as rec:
            assert await escalate_due(web_alive=True, _due=[_due()]) == 0
        rec.assert_called_once()
        assert rec.call_args.kwargs["delivered"] == 0

    @pytest.mark.asyncio
    async def test_a_broken_archive_never_takes_the_canary_down(
        self, monkeypatch,
    ):
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@y/z")
        with patch("core.alert_escalator._query_due",
                   new=AsyncMock(side_effect=ConnectionError("refused"))):
            assert await escalate_due(web_alive=True) == 0


class TestTheCycleRule:
    """The SQL owns 'one escalation per firing cycle'; what a unit test can
    pin is the shape the query promises, so a rewrite that drops a clause
    fails loudly here rather than quietly in production."""

    def test_the_query_carries_the_three_guards(self):
        from core.alert_escalator import _DUE_QUERY

        q = _DUE_QUERY
        assert "acknowledged_at IS NULL" in q          # П4 hook
        assert "state = 'firing'" in q                 # resolved never escalates
        assert "kind = 'condition'" in q               # events never escalate
        assert "event_type = 'escalated'" in q         # once per cycle
        assert "event_type = 'resolved'" in q          # cycle boundary


class TestFormat:
    def test_the_message_names_age_and_count(self):
        text = format_escalation([_due(hours=26, fired=4)])
        assert "26ч" in text and "×4" in text
        assert "Висит без реакции" in text

    def test_naive_timestamps_are_read_as_utc(self):
        row = DueCondition(
            condition_key="k", fired_count=1,
            cycle_start=datetime.utcnow() - timedelta(hours=8),
        )
        assert "8ч" in format_escalation([row])
