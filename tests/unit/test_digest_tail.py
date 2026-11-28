"""Step 06: the ledger's tail on the digest.

The delta stays on data_quality_issues — a metric defined twice drifts. The
archive contributes only what it alone knows: what stands firing, for how
long, what was escalated, how many are acknowledged. And it rides a digest
that news already earned — it never summons one.
"""
from datetime import date, datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

from core.alert_archive import render_digest_tail
from tests.helpers.alerts_journal_pg import alert_ledger
from tests.helpers.alerts_telegram import ADMINS, telegram_at_the_transport
from tests.helpers.dq_journal_pg import pool  # noqa: F401 — the journal, emptied
from tests.integration.pg_tab_seed import needs_postgres


def _t(hours_ago):
    return datetime.now(timezone.utc) - timedelta(hours=hours_ago)


class TestRender:
    def test_a_quiet_ledger_renders_nothing(self):
        """An empty block every morning teaches readers to skip the tail."""
        assert render_digest_tail([], 0, 0) is None

    def test_firing_rows_carry_age_count_and_escalation(self):
        tail = render_digest_tail(
            [("mirror_failing", _t(30), 3, True),
             ("disk:WARN", _t(2), 1, False)],
            resolved_24h=0, acknowledged=0,
        )
        assert "Alert ledger" in tail
        assert "mirror_failing — 30h, ×3 · escalated" in tail
        assert "disk:WARN — 2h, ×1" in tail
        assert "Resolved" not in tail

    def test_old_conditions_age_in_days(self):
        tail = render_digest_tail([("k", _t(75), 9, False)], 0, 0)
        assert "3d" in tail

    def test_resolved_and_acknowledged_lines_render_only_when_nonzero(self):
        tail = render_digest_tail([], resolved_24h=2, acknowledged=1)
        assert "Resolved in 24h: 2" in tail
        assert "Acknowledged: 1" in tail
        assert "Firing" not in tail

    def test_the_firing_list_is_capped(self):
        rows = [(f"k{i}", _t(10), 1, False) for i in range(14)]
        tail = render_digest_tail(rows, 0, 0)
        assert "+4 more" in tail

    def test_naive_timestamps_read_as_utc(self):
        tail = render_digest_tail(
            [("k", datetime.utcnow() - timedelta(hours=5), 1, False)], 0, 0)
        assert "5h" in tail


async def _integrity_run(minutes_ago: int, count: int) -> None:
    """One integrity run `minutes_ago`, its one finding a WARN at `count`."""
    from core import dq_journal
    from core.data_quality import IntegrityIssue, Severity

    at = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    await dq_journal.journal_run(
        started_at=at, ended_at=at, as_of=at,
        window_start=date(2026, 1, 1), window_end=date(2026, 8, 15),
        layer="integrity", discrepancies=[],
        issues=[IntegrityIssue(
            check_name="pg_headline_vs_line_items", table_name="silver_orders",
            severity=Severity.WARN, count=count, sample_ids=(1, 2),
            description=f"{count} order(s) have grand_total = 0")])


async def _every_other_layer_clean() -> None:
    """A clean run of every other watched layer half an hour ago: a layer that
    never ran is news of its own, and would send every digest here."""
    from core import dq_journal
    from core.data_quality import WATCHED_LAYERS

    at = datetime.now(timezone.utc) - timedelta(minutes=30)
    for layer in WATCHED_LAYERS:
        if layer != "integrity":
            await dq_journal.journal_run(
                started_at=at, ended_at=at, as_of=at,
                window_start=date(2026, 1, 1), window_end=date(2026, 8, 15),
                layer=layer, issues=[], discrepancies=[])


class TestDigestWiring:
    @needs_postgres
    @pytest.mark.asyncio
    async def test_the_tail_rides_a_digest_and_never_summons_one(self, pool, monkeypatch):
        """A condition stands firing in the ledger through two digests. The
        first has no news — the one finding unchanged across both runs since
        the last digest went out — and nothing is sent, the standing condition
        notwithstanding. The second has news, the finding grew, and the one
        message each admin gets carries the tail, naming the condition.
        Mutation: fetch and send the tail on a quiet day, or leave it off the
        digest."""
        from core import dq_journal
        from core.scheduler import BackgroundScheduler
        from core.telegram_alerts import reset_throttle

        telegram = telegram_at_the_transport(monkeypatch)
        reset_throttle()
        try:
            async with alert_ledger(monkeypatch) as ledger:
                await ledger.firing("disk:WARN", since=timedelta(hours=7))
                await dq_journal.mark_digest_sent(
                    (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat())
                await _every_other_layer_clean()
                await _integrity_run(45, 414)
                await _integrity_run(5, 414)
                quiet = await BackgroundScheduler()._run_dq_digest()
                sent_when_quiet = list(telegram.messages)

                await _integrity_run(1, 415)
                news = await BackgroundScheduler()._run_dq_digest()
        finally:
            reset_throttle()

        assert (quiet["sent"], quiet["quiet"]) == (False, True)
        assert sent_when_quiet == []
        assert news["sent"] is True
        assert telegram.chats() == sorted(ADMINS)
        assert all("disk:WARN" in p["text"] for p in telegram.messages)

    @pytest.mark.asyncio
    async def test_fetch_failure_never_sinks_the_digest(self, monkeypatch):
        """A dead ledger costs the tail, not the message."""
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x@nowhere/ks")
        from core import alert_archive

        with patch("core.pg.get_pool",
                   new=AsyncMock(side_effect=ConnectionError("refused"))):
            assert await alert_archive.fetch_digest_tail() is None

    @pytest.mark.asyncio
    async def test_no_dsn_no_tail(self, monkeypatch):
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        from core import alert_archive

        assert await alert_archive.fetch_digest_tail() is None
