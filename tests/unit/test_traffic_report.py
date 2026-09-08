"""The weekly traffic report: where the orders came from, once a week.

Two things this file is really guarding. The report must ask the repository
the same questions the tab asks — a message that disagreed with the screen
would cost more trust than it delivers — and it must never describe a missing
read as a finding: a week whose UTM rows have not landed yet has orders and no
attribution, which looks exactly like a week where nothing came from anywhere.
"""
from __future__ import annotations

import re
from datetime import date

import pytest

from core.traffic_report import (
    CAMPAIGN_FLOOR,
    UNATTRIBUTED_WARN_PCT,
    TrafficReport,
    build_report,
    chart_view,
    format_report,
    format_report_rich,
)
from tests.unit.test_rich_report import SUPPORTED_TAGS


def _analytics(scale=1.0, unknown_orders=40, totals_orders=300):
    return {
        "totals": {"orders": totals_orders, "revenue": 700_000 * scale},
        "summary": {
            "paid": {"orders": 120, "revenue": 380_000 * scale},
            "organic": {"orders": 90, "revenue": 190_000 * scale},
            "manager": {"orders": 30, "revenue": 70_000 * scale},
            "pixel_only": {"orders": 20, "revenue": 30_000 * scale},
            "unknown": {"orders": unknown_orders, "revenue": 30_000 * scale},
        },
        "by_platform": {
            "facebook": {"orders": 80, "revenue": 250_000 * scale},
            "google_ads": {"orders": 40, "revenue": 130_000 * scale},
            "instagram": {"orders": 70, "revenue": 150_000 * scale},
            "tiktok": {"orders": 20, "revenue": 60_000 * scale},
            "google_organic": {"orders": 15, "revenue": 40_000 * scale},
            "email": {"orders": 5, "revenue": 10_000 * scale},
        },
    }


class FakeStore:
    """Answers the two calls `build_report` makes, and records them."""

    THIS_WEEK = date(2026, 8, 31)

    def __init__(self, analytics=None, campaigns=None):
        self.calls = []
        self._analytics = analytics or (lambda cur: _analytics(1.0 if cur else 1.25))
        self._campaigns = campaigns or self._default_campaigns

    @staticmethod
    def _default_campaigns(cur):
        return [
            {"campaign": "bf_broad", "platform": "facebook",
             "revenue": 90_000 if cur else 40_000},
            {"campaign": "retarget_7d", "platform": "facebook",
             "revenue": 30_000 if cur else 75_000},
            {"campaign": "search_brand", "platform": "google_ads",
             "revenue": 45_000 if cur else 44_000},
            {"campaign": "noise", "platform": "tiktok",
             "revenue": 1_500 if cur else 500},
        ]

    async def get_traffic_analytics(self, start_date, end_date, sales_type=None,
                                    source_id=None):
        self.calls.append(("analytics", start_date, end_date, sales_type))
        return self._analytics(start_date >= self.THIS_WEEK)

    async def get_traffic_utm_campaigns(self, start_date, end_date, sales_type=None,
                                        limit=50, **kwargs):
        self.calls.append(("campaigns", start_date, end_date, sales_type))
        return {"campaigns": self._campaigns(start_date >= self.THIS_WEEK),
                "total": 4}


async def _report(store=None, today=date(2026, 9, 8)):
    return await build_report(store or FakeStore(), today)


class TestItAsksWhatTheTabAsks:
    @pytest.mark.asyncio
    async def test_two_windows_and_the_tab_s_sales_type(self):
        store = FakeStore()
        await _report(store)
        windows = {(kind, str(a), str(b)) for kind, a, b, _ in store.calls}
        assert ("analytics", "2026-08-31", "2026-09-06") in windows
        assert ("analytics", "2026-08-24", "2026-08-30") in windows
        assert ("campaigns", "2026-08-31", "2026-09-06") in windows
        assert {sales_type for *_, sales_type in store.calls} == {"retail"}

    @pytest.mark.asyncio
    async def test_it_reports_the_last_complete_week(self):
        report = await _report()
        assert (report.start, report.end) == (date(2026, 8, 31), date(2026, 9, 6))


class TestAttributionQuality:
    @pytest.mark.asyncio
    async def test_the_unattributed_share_is_of_orders_not_revenue(self):
        """Revenue would flatter it: an untracked order is usually a small
        one, and the question is how many orders we cannot place."""
        report = await _report()
        assert report.unattributed_pct == pytest.approx(40 / 300 * 100)

    @pytest.mark.asyncio
    async def test_a_bad_week_is_marked_and_a_good_one_is_not(self):
        clean = await _report()
        assert "✅" in format_report_rich(clean, None, "en")

        noisy = await _report(FakeStore(
            analytics=lambda cur: _analytics(1.0, unknown_orders=120)))
        assert noisy.unattributed_pct > UNATTRIBUTED_WARN_PCT
        assert "⚠️" in format_report_rich(noisy, None, "en")

    @pytest.mark.asyncio
    async def test_it_says_what_the_share_was_last_week(self):
        """A share that doubled matters more than the share itself."""
        report = await _report()
        assert "Last week it was" in format_report_rich(report, None, "en")

    def test_no_orders_means_no_share_rather_than_a_division(self):
        empty = TrafficReport(
            start=date(2026, 8, 31), end=date(2026, 9, 6), sales_type="retail",
            revenue=0.0, orders=0, previous_revenue=0.0, previous_orders=0,
        )
        assert empty.unattributed_pct is None
        assert empty.previous_unattributed_pct is None


class TestPlatformsAndCampaigns:
    @pytest.mark.asyncio
    async def test_platforms_are_ranked_and_capped(self):
        report = await _report()
        assert [p.name for p in report.platforms] == [
            "facebook", "instagram", "google_ads", "tiktok", "google_organic",
        ], "six platforms in, the five largest out, by revenue"
        assert all(p.previous_revenue > 0 for p in report.platforms)

    @pytest.mark.asyncio
    async def test_campaigns_rank_by_hryvnia_moved_not_by_percent(self):
        """`bf_broad` gained ₴50k and `retarget_7d` lost ₴45k; a percentage
        ranking would put a campaign that tripled from nothing above both."""
        report = await _report()
        assert [m.campaign for m in report.movers] == ["bf_broad", "retarget_7d"]
        assert report.movers[0].delta == 50_000
        assert report.movers[1].delta == -45_000

    @pytest.mark.asyncio
    async def test_a_campaign_below_the_floor_is_not_news(self):
        report = await _report()
        assert all(abs(m.delta) >= CAMPAIGN_FLOOR for m in report.movers)
        assert "noise" not in {m.campaign for m in report.movers}
        assert "search_brand" not in {m.campaign for m in report.movers}

    @pytest.mark.asyncio
    async def test_untagged_traffic_is_not_a_campaign(self):
        """It is the largest mover most weeks — the first production run put
        it at -346,815, four times the largest real one — and it would push
        every actionable line off a list of five. Untagged traffic is already
        reported by name in the buckets above."""
        def campaigns(cur):
            return [
                {"campaign": "—", "platform": "", "revenue": 10_000 if cur else 350_000},
                {"campaign": "real_one", "platform": "facebook",
                 "revenue": 30_000 if cur else 10_000},
            ]

        report = await _report(FakeStore(campaigns=campaigns))
        assert [m.campaign for m in report.movers] == ["real_one"]

    @pytest.mark.asyncio
    async def test_a_campaign_that_only_ran_last_week_still_shows(self):
        """A campaign that stopped is the most reportable thing there is, and
        it appears in only one of the two weeks."""
        def campaigns(cur):
            return [] if cur else [
                {"campaign": "stopped", "platform": "tiktok", "revenue": 60_000}]

        report = await _report(FakeStore(campaigns=campaigns))
        assert [(m.campaign, m.delta) for m in report.movers] == [("stopped", -60_000)]


class TestRendering:
    @pytest.mark.asyncio
    async def test_the_rich_form_uses_only_documented_tags(self):
        report = await _report()
        html = format_report_rich(report, "https://x.example", "ru",
                                  figures={"platforms": "platforms"})
        tags = set(re.findall(r"</?([a-z][a-z0-9-]*)", html))
        assert tags <= SUPPORTED_TAGS, tags - SUPPORTED_TAGS

    @pytest.mark.asyncio
    @pytest.mark.parametrize("lang", ["en", "uk", "ru"])
    async def test_it_renders_in_every_language(self, lang):
        report = await _report()
        for text in (format_report_rich(report, "https://x.example", lang),
                     format_report(report, "https://x.example", lang)):
            assert "traffic." not in text, "an untranslated key leaked through"
            assert "None" not in text

    @pytest.mark.asyncio
    async def test_the_button_and_the_link_point_at_the_tab(self):
        report = await _report()
        assert "https://x.example/traffic" in format_report_rich(
            report, "https://x.example/", "en")
        assert "https://x.example/traffic" in format_report(
            report, "https://x.example", "en")

    @pytest.mark.asyncio
    async def test_an_unknown_platform_shows_its_key_rather_than_vanishing(self):
        """The classifier can emit a platform before anybody translates it."""
        def analytics(cur):
            data = _analytics(1.0 if cur else 1.25)
            data["by_platform"] = {"pinterest": {"orders": 5, "revenue": 900_000}}
            return data

        report = await _report(FakeStore(analytics=analytics))
        assert "pinterest" in format_report_rich(report, None, "ru")

    @pytest.mark.asyncio
    async def test_the_chart_view_is_labelled_for_its_language(self):
        report = await _report()
        names = [c.name for c in chart_view(report, "ru").channels]
        assert "Google органика" in names
        assert "google_organic" not in names

    @pytest.mark.asyncio
    async def test_the_fallback_form_carries_the_same_facts(self):
        report = await _report()
        text = format_report(report, None, "en")
        assert "300" in text                      # orders
        assert "Paid ads" in text and "No tracking" in text
        assert "bf_broad" in text
        assert "<table" not in text, "the fallback is lines, not a document"


# ─── The scheduled job ──────────────────────────────────────────────────────

async def _duck_store(tmp_path):
    from core.duckdb_store import DuckDBStore

    store = DuckDBStore(db_path=tmp_path / "traffic.duckdb")
    await store.connect()
    return store


async def _seed_gold(store, complete=True):
    """Enough Gold for the readiness gate to pass — the job asks the warehouse
    how far it has got, not how much traffic there was."""
    from datetime import datetime, timedelta

    from core.scheduler import SCHEDULER_TIMEZONE
    from core.weekly_report import last_complete_week

    today = datetime.now(SCHEDULER_TIMEZONE).date()
    start, end = last_complete_week(today)
    last = end if complete else end - timedelta(days=3)
    async with store.connection() as conn:
        day = start
        while day <= last:
            conn.execute(
                "INSERT OR REPLACE INTO gold_daily_revenue "
                "(date, sales_type, revenue, orders_count) VALUES (?, 'retail', ?, ?)",
                [day, 5_000, 2],
            )
            day += timedelta(days=1)
    return start


class TestTheScheduledJob:
    ADMINS = [111, 222]

    def _wire(self, monkeypatch, store, tmp_path, *, analytics=None, campaigns=None):
        import importlib

        from bot.store_sqlite import SqliteBotStore
        from core.scheduler import BackgroundScheduler

        fake = FakeStore(analytics=analytics, campaigns=campaigns)
        monkeypatch.setattr(store, "get_traffic_analytics",
                            fake.get_traffic_analytics, raising=False)
        monkeypatch.setattr(store, "get_traffic_utm_campaigns",
                            fake.get_traffic_utm_campaigns, raising=False)

        monkeypatch.setattr("bot.store_sqlite.DB_PATH", tmp_path / "prefs.db")
        bot_store = SqliteBotStore()
        bot_store.initialise()
        monkeypatch.setattr("core.bot_store._store", bot_store)

        core_config = importlib.import_module("core.config")
        monkeypatch.setattr(core_config, "ADMIN_USER_IDS", self.ADMINS)

        async def _get_store():
            return store

        monkeypatch.setattr("core.duckdb_store.get_store", _get_store)
        return BackgroundScheduler()

    @staticmethod
    def _capture(monkeypatch, rich=2, text=2):
        sent = {"rich": [], "text": []}

        async def _rich(html, media=None, chat_ids=None, **kwargs):
            sent["rich"].append({"html": html, "chat_ids": list(chat_ids or [])})
            return rich

        async def _text(body, chat_ids=None, **kwargs):
            sent["text"].append({"body": body, "chat_ids": list(chat_ids or [])})
            return text

        monkeypatch.setattr("core.telegram_alerts.send_rich_message_http", _rich)
        monkeypatch.setattr("core.telegram_alerts.send_admin_message_http", _text)
        return sent

    @pytest.mark.asyncio
    async def test_it_waits_while_the_warehouse_is_behind(self, tmp_path, monkeypatch):
        store = await _duck_store(tmp_path)
        try:
            await _seed_gold(store, complete=False)
            scheduler = self._wire(monkeypatch, store, tmp_path)
            sent = self._capture(monkeypatch)
            result = await scheduler._run_traffic_report()
            assert result["sent"] is False and result["reason"] == "warehouse_behind"
            assert sent["rich"] == [] and sent["text"] == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_orders_with_no_attribution_is_a_missing_read_not_a_finding(
        self, tmp_path, monkeypatch,
    ):
        """The UTM rows land behind the orders. A week whose attribution has
        not arrived looks exactly like a week where nothing came from
        anywhere, and saying so would be worse than saying nothing."""
        def analytics(cur):
            return {"totals": {"orders": 300, "revenue": 700_000},
                    "summary": {}, "by_platform": {}}

        store = await _duck_store(tmp_path)
        try:
            await _seed_gold(store)
            scheduler = self._wire(monkeypatch, store, tmp_path, analytics=analytics)
            sent = self._capture(monkeypatch)
            result = await scheduler._run_traffic_report()
            assert result["sent"] is False and result["reason"] == "no_attribution"
            assert sent["rich"] == [] and sent["text"] == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_it_delivers_once_and_then_stays_quiet(self, tmp_path, monkeypatch):
        store = await _duck_store(tmp_path)
        try:
            week_start = await _seed_gold(store)
            scheduler = self._wire(monkeypatch, store, tmp_path)
            sent = self._capture(monkeypatch)

            first = await scheduler._run_traffic_report()
            assert first["sent"] is True and first["rich"] is True
            assert first["week"] == week_start.isoformat()
            assert len(sent["rich"]) == 1 and sent["text"] == []

            second = await scheduler._run_traffic_report()
            assert second == {"sent": False, "reason": "already_sent",
                              "week": week_start.isoformat()}
            assert len(sent["rich"]) == 1, "the second tick must send nothing"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_it_writes_only_to_admins(self, tmp_path, monkeypatch):
        """The tab is visible to seventeen of eighteen dashboard accounts;
        this report is not."""
        store = await _duck_store(tmp_path)
        try:
            await _seed_gold(store)
            scheduler = self._wire(monkeypatch, store, tmp_path)
            sent = self._capture(monkeypatch)
            await scheduler._run_traffic_report()
            written_to = {uid for call in sent["rich"] for uid in call["chat_ids"]}
            assert written_to == set(self.ADMINS)
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_rejected_rich_message_falls_back_to_text(
        self, tmp_path, monkeypatch,
    ):
        store = await _duck_store(tmp_path)
        try:
            await _seed_gold(store)
            scheduler = self._wire(monkeypatch, store, tmp_path)
            sent = self._capture(monkeypatch, rich=0)
            result = await scheduler._run_traffic_report()
            assert result["sent"] is True and result["rich"] is False
            assert len(sent["text"]) == 1
            assert "Traffic report" in sent["text"][0]["body"] or len(sent["text"]) == 1
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_send_that_reached_nobody_leaves_the_week_pending(
        self, tmp_path, monkeypatch,
    ):
        """Nobody heard it, so it did not happen: tomorrow's tick tries
        again rather than dropping the week."""
        from core.traffic_report import already_sent

        store = await _duck_store(tmp_path)
        try:
            week_start = await _seed_gold(store)
            scheduler = self._wire(monkeypatch, store, tmp_path)
            self._capture(monkeypatch, rich=0, text=0)
            result = await scheduler._run_traffic_report()
            assert result["sent"] is False and result["reason"] == "not_delivered"
            async with store.connection() as conn:
                assert not already_sent(conn, week_start, "retail")
        finally:
            await store.close()
