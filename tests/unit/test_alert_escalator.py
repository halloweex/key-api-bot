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
from tests.helpers.alerts_journal_pg import alert_ledger
from tests.helpers.alerts_telegram import ADMINS, telegram_at_the_transport
from tests.integration.pg_tab_seed import needs_postgres


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
        keys = ["mirror_failing", "disk:CRITICAL"]
        with patch("bot.main.send_admin_message",
                   new=AsyncMock(return_value=2)) as send, \
             patch("core.alert_archive.write_escalated_now",
                   new=AsyncMock(return_value=keys)) as rec:
            assert await escalate_due(web_alive=True, _due=rows) == 2
        text = send.await_args.args[0]
        assert "mirror_failing — 7h, fired ×3" in text
        assert "disk:CRITICAL — 9h" in text
        assert send.await_args.kwargs["pre_throttled"] is True
        assert sorted(rec.await_args.args[0]) == [
            "disk:CRITICAL", "mirror_failing"]

    @needs_postgres
    @pytest.mark.asyncio
    async def test_recorded_before_delivery_whatever_its_outcome(self, monkeypatch):
        """The escalated event is what stops the next 15-minute tick from
        re-judging the same cycle — a send nobody received must not retry
        forever. Written first and awaited, so a slow ledger cannot leave the
        row missing behind a message that already went: when the message
        leaves, the row is there."""
        telegram = telegram_at_the_transport(monkeypatch)
        async with alert_ledger(monkeypatch) as ledger:
            await ledger.firing("freshness_orders", since=timedelta(hours=7))
            rows_when_sent = []

            async def at_send(_payload):
                rows_when_sent.append(await ledger.count("escalated"))

            telegram.on_send = at_send
            telegram.status = 403              # every admin refuses
            assert await escalate_due(web_alive=True) == 0
            telegram.status = 200              # the next tick, Telegram back
            assert await escalate_due(web_alive=True) == 0
            escalated = [e for e in await ledger.events() if e[1] == "escalated"]

        assert rows_when_sent == [1] * len(ADMINS)
        assert len(telegram.attempts) == len(ADMINS)  # the second tick tried nobody
        assert escalated == [("freshness_orders", "escalated", None)]

    @pytest.mark.asyncio
    async def test_a_ledger_that_cannot_record_holds_the_escalation(self, monkeypatch):
        """Better a late escalation than the same one every fifteen minutes."""
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@y/z")
        with patch("bot.main.send_admin_message", new=AsyncMock(return_value=2)) as send, \
             patch("core.alert_archive.write_escalated_now", new=AsyncMock(return_value=None)):
            assert await escalate_due(web_alive=True, _due=[_due()]) == 0
        send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_condition_resolved_since_the_snapshot_is_not_escalated(self, monkeypatch):
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@y/z")
        rows = [_due("mirror_failing", 7, 3), _due("disk:CRITICAL", 9, 2)]
        with patch("bot.main.send_admin_message", new=AsyncMock(return_value=2)) as send, \
             patch("core.alert_archive.write_escalated_now",
                   new=AsyncMock(return_value=["disk:CRITICAL"])):
            assert await escalate_due(web_alive=True, _due=rows) == 2
        text = send.await_args.args[0]
        assert "disk:CRITICAL" in text and "mirror_failing" not in text

    @pytest.mark.asyncio
    async def test_a_broken_archive_never_takes_the_canary_down(
        self, monkeypatch,
    ):
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@y/z")
        with patch("core.alert_escalator._query_due",
                   new=AsyncMock(side_effect=ConnectionError("refused"))):
            assert await escalate_due(web_alive=True) == 0


class TestFormat:
    def test_the_message_names_age_and_count(self):
        text = format_escalation([_due(hours=26, fired=4)])
        assert "26h" in text and "×4" in text
        assert "Still firing" in text

    def test_naive_timestamps_are_read_as_utc(self):
        row = DueCondition(
            condition_key="k", fired_count=1,
            cycle_start=datetime.utcnow() - timedelta(hours=8),
        )
        assert "8h" in format_escalation([row])
