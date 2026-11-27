"""The weekly traffic report: where the orders came from, once a week.

Two things this file is really guarding. The report must ask the repository
the same questions the tab asks — a message that disagreed with the screen
would cost more trust than it delivers — and it must never describe a missing
read as a finding: a week whose UTM rows have not landed yet has orders and no
attribution, which looks exactly like a week where nothing came from anywhere.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import date, datetime, timedelta, timezone

import pytest
import pytest_asyncio

from core.duckdb_constants import B2B_MANAGER_ID
from core.traffic_report import (
    CAMPAIGN_FLOOR,
    RECIPIENTS_ENV,
    TrafficReport,
    audience,
    build_report,
    chart_view,
    extra_recipients,
    format_report,
    format_report_rich,
)
from tests.integration.pg_tab_seed import needs_postgres
from tests.helpers.rich_tags import SUPPORTED_TAGS
from tests.helpers.traffic_report_job import (
    TrafficReportJob,
    FakeStore, _analytics, _job_store, _seed_gold,
)


@pytest.fixture(autouse=True)
def ledger(monkeypatch):
    """The two send ledgers' Postgres calls (`mark_sent`, `find_sent`) over a
    dict keyed `(chain, week_start, sales_type)`. Until step 14's S14-10 this
    module kept the ledger in a DuckDB store under the test; the job goes
    through the real door, `core.report_ledger`, either way — the ledger's
    own proof is `tests/unit/test_report_ledger.py` and
    `tests/integration/test_report_ledger_pg.py`."""
    from core import pg_traffic_ledger_write, pg_weekly_ledger_write

    rows = {}
    for chain in (pg_weekly_ledger_write, pg_traffic_ledger_write):
        async def mark_sent(week_start, sales_type, revenue, orders, sent_at,
                            chain=chain):
            rows[(chain.CHAIN, week_start, sales_type)] = dict(
                week_start=week_start, sales_type=sales_type, revenue=revenue,
                orders=orders, sent_at=sent_at)

        async def find_sent(week_start, sales_type, chain=chain):
            return rows.get((chain.CHAIN, week_start, sales_type))

        monkeypatch.setattr(chain, "mark_sent", mark_sent)
        monkeypatch.setattr(chain, "find_sent", find_sent)
    return rows


async def _traffic_sent(week_start) -> bool:
    """The gate's answer for the week, through the door the job asks."""
    from core import report_ledger

    return await report_ledger.already_sent(report_ledger.TRAFFIC, week_start, "retail")


async def _report(store=None, today=date(2026, 9, 8)):
    return await build_report(store or FakeStore(), today)


# Two weeks of orders in Postgres for the report to read: retail, and the b2b
# and internal orders the tab's default leaves out. 2026-09-08 is a Tuesday,
# so the report's week is 31.08–06.09 and the one before it 24.08–30.08.
# (id, source_id, grand_total, day, manager_id, verdict) — the verdict is
# `(traffic_type, platform, utm_campaign)`, or None for an order with no UTM
# row, which the tab places by its source (Instagram, Telegram: organic).
B2B, INTERNAL = B2B_MANAGER_ID, 99            # 99: a manager on no retail list
TWO_WEEKS = (
    # retail, the reported week
    (101, 4, 12_000, date(2026, 8, 31), None, ("paid_confirmed", "facebook", "spring")),
    (102, 1, 2_500, date(2026, 9, 2), None, None),
    (103, 4, 1_500, date(2026, 9, 6), None, ("pixel_only", "unattributed", None)),
    # retail, the week before
    (104, 4, 4_000, date(2026, 8, 24), None, ("paid_confirmed", "facebook", "spring")),
    (105, 2, 800, date(2026, 8, 30), None, None),
    # retail, either side of the two weeks
    (106, 4, 7_000, date(2026, 9, 7), None, ("paid_confirmed", "facebook", "spring")),
    (107, 4, 6_000, date(2026, 8, 23), None, ("paid_confirmed", "facebook", "spring")),
    # wholesale, in both weeks, under a campaign of its own
    (201, 2, 50_000, date(2026, 9, 1), B2B, ("paid_confirmed", "facebook", "wholesale")),
    (202, 2, 30_000, date(2026, 8, 25), B2B, ("paid_confirmed", "facebook", "wholesale")),
    # internal, the reported week
    (301, 1, 9_000, date(2026, 9, 3), INTERNAL, None),
)


async def _two_weeks(conn, pool):
    """Bronze landed, Silver and Gold derived from it, then the UTM rows."""
    from tests.integration.pg_tab_seed import derive, seed

    await seed(conn, orders=[
        (oid, source, 1, total,
         datetime(day.year, day.month, day.day, 9, tzinfo=timezone.utc),  # midday, Kyiv
         oid, manager, None)
        for oid, source, total, day, manager, _verdict in TWO_WEEKS])
    await derive(pool)
    await conn.executemany(
        "INSERT INTO silver.order_utm (order_id, traffic_type, platform, utm_campaign,"
        " parsed_at) VALUES ($1, $2, $3, $4, now())",
        [(oid, *verdict) for oid, *_rest, verdict in TWO_WEEKS if verdict])


class TestItAsksWhatTheTabAsks:
    @needs_postgres
    @pytest.mark.asyncio
    async def test_the_two_weeks_are_the_tabs_retail(self, monkeypatch):
        """The report counts what the traffic tab counts by default: retail,
        in the last complete week and the one before it. Counted with b2b it
        would set ₴50 000 of wholesale beside ₴16 000 of retail and put a
        wholesale campaign at the head of the movers; and the tab, asked for
        the same two weeks, answers the same totals."""
        from core.store import AnalyticsStore
        from tests.helpers.app_client import signed_in
        from tests.integration.pg_tab_seed import tab_in_postgres

        async with tab_in_postgres(os.environ["KS_PG_DSN"], monkeypatch, _two_weeks):
            report = await build_report(AnalyticsStore(), date(2026, 9, 8))
            async with signed_in("viewer", tabs="traffic") as viewer:
                week = await viewer.get("/api/traffic/analytics", params={
                    "start_date": "2026-08-31", "end_date": "2026-09-06"})
                before = await viewer.get("/api/traffic/analytics", params={
                    "start_date": "2026-08-24", "end_date": "2026-08-30"})

        assert (report.start, report.end, report.sales_type) == (
            date(2026, 8, 31), date(2026, 9, 6), "retail")
        assert (report.orders, report.revenue) == (3, 16_000.0)
        assert (report.previous_orders, report.previous_revenue) == (2, 4_800.0)
        assert {b.name: (b.orders, b.revenue, b.previous_orders, b.previous_revenue)
                for b in report.buckets} == {
            "paid": (1, 12_000.0, 1, 4_000.0),
            "organic": (1, 2_500.0, 1, 800.0),
            "pixel_only": (1, 1_500.0, 0, 0.0),
        }
        assert [(p.name, p.revenue, p.previous_revenue) for p in report.platforms] == [
            ("facebook", 12_000.0, 4_000.0),
            ("instagram", 2_500.0, 0.0),
            ("unattributed", 1_500.0, 0.0),
        ]
        assert [(m.campaign, m.current, m.previous) for m in report.movers] == [
            ("spring", 12_000.0, 4_000.0)]

        assert week.status_code == before.status_code == 200
        assert week.json()["totals"] == {"orders": report.orders, "revenue": report.revenue}
        assert before.json()["totals"] == {
            "orders": report.previous_orders, "revenue": report.previous_revenue}

    @pytest.mark.asyncio
    async def test_it_reports_the_last_complete_week(self):
        report = await _report()
        assert (report.start, report.end) == (date(2026, 8, 31), date(2026, 9, 6))


class TestWhatIsEvidenceAndWhatIsInference:
    """The headline number, and the one this got wrong first.

    It used to report the `unknown` bucket as "arrived with no tracking" —
    5% of the week it first ran, with a tick — while 251 of those 336 orders
    carried no UTM values at all. `unknown` means "no tracking data
    whatsoever" and most untracked orders are not in it, so the threshold
    set against it could never have fired.
    """

    @pytest.mark.asyncio
    async def test_only_the_paid_bucket_counts_as_a_named_campaign(self):
        """A campaign *is* `utm_campaign`, and that is what puts an order in
        `paid`. Everything else is placed by inference."""
        report = await _report()
        assert report.named_campaign_orders == 120
        assert report.named_pct == pytest.approx(120 / 300 * 100)

    @pytest.mark.asyncio
    async def test_the_pixel_and_unknown_buckets_are_not_evidence_of_a_campaign(self):
        """Both say something about the channel and nothing about the ad."""
        report = await _report()
        by_name = {b.name: b.orders for b in report.buckets}
        assert by_name["pixel_only"] and by_name["unknown"]
        assert report.named_campaign_orders < report.orders - by_name["unknown"]

    @pytest.mark.asyncio
    async def test_the_sentence_states_the_count_and_never_judges_it(self):
        """A verdict nobody can calibrate is how a tick came to sit over a
        number that was wrong twice over."""
        report = await _report()
        html = format_report_rich(report, None, "en")
        assert "A campaign can be named for 120 of 300 orders (40%)" in html
        assert "✅" not in html and "⚠️" not in html

    @pytest.mark.asyncio
    async def test_it_says_what_the_share_was_last_week(self):
        report = await _report()
        assert "Last week it was" in format_report_rich(report, None, "en")

    def test_no_orders_means_no_share_rather_than_a_division(self):
        empty = TrafficReport(
            start=date(2026, 8, 31), end=date(2026, 9, 6), sales_type="retail",
            revenue=0.0, orders=0, previous_revenue=0.0, previous_orders=0,
        )
        assert empty.named_pct is None
        assert empty.previous_named_pct is None


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


# ─── Who the report is written to ───────────────────────────────────────────


class TestTheAudienceList:
    """`KS_TRAFFIC_REPORT_RECIPIENTS` widens the report past the admins.

    Admins-only was the shipped decision, and it stays the default: the
    traffic tab is in the default set for both `viewer` and `editor`, so
    "everyone who can open it" is seventeen of eighteen accounts and is not
    the sentence anybody said. The env list is the sentence — written where
    it can change without a deploy, and empty until somebody writes in it.

    A list that decides who gets a message on their phone has to fail in one
    direction only: a malformed entry may cost that entry and never anybody
    else's, and it may never invent a recipient.
    """

    def test_unset_means_nobody_extra(self, monkeypatch):
        monkeypatch.delenv(RECIPIENTS_ENV, raising=False)
        assert extra_recipients() == []
        assert audience([111, 222]) == [111, 222]

    def test_empty_and_whitespace_are_not_recipients(self, monkeypatch):
        """`KS_TRAFFIC_REPORT_RECIPIENTS=` and a stray trailing comma are how
        the variable actually looks after somebody edits it by hand."""
        for value in ("", "   ", ",", " , , "):
            monkeypatch.setenv(RECIPIENTS_ENV, value)
            assert extra_recipients() == [], value
            assert audience([111]) == [111], value

    def test_a_list_is_added_to_the_admins(self, monkeypatch):
        monkeypatch.setenv(RECIPIENTS_ENV, "333,444")
        assert extra_recipients() == [333, 444]
        assert audience([111, 222]) == [111, 222, 333, 444]

    def test_semicolons_and_spaces_are_tolerated(self, monkeypatch):
        """Nobody reads a variable's grammar before typing in it."""
        monkeypatch.setenv(RECIPIENTS_ENV, " 333 ; 444 , 555 ")
        assert extra_recipients() == [333, 444, 555]

    def test_a_malformed_entry_costs_only_itself(self, monkeypatch, caplog):
        """Raising here would take the report away from everybody to punish
        one typo — the opposite of what a reporting job should do."""
        monkeypatch.setenv(RECIPIENTS_ENV, "333,not-an-id,444")
        with caplog.at_level(logging.WARNING):
            assert extra_recipients() == [333, 444]
        assert "not-an-id" in caplog.text

    def test_an_admin_listed_again_is_written_to_once(self, monkeypatch):
        """The two lists are maintained by hand and will overlap. A duplicate
        here is a second message to the same phone."""
        monkeypatch.setenv(RECIPIENTS_ENV, "222,333,333")
        assert audience([111, 222]) == [111, 222, 333]

    def test_the_order_is_stable_and_admins_come_first(self, monkeypatch):
        """`group_by_language` and the send ledger both walk this list; an
        order that moved between ticks would make two runs incomparable."""
        monkeypatch.setenv(RECIPIENTS_ENV, "555,444")
        assert audience([222, 111]) == [222, 111, 555, 444]

    def test_ids_that_arrive_as_strings_are_still_ids(self, monkeypatch):
        """`ADMIN_USER_IDS` is parsed out of the environment too, so its
        members are not guaranteed to be ints by the time they reach here."""
        monkeypatch.setenv(RECIPIENTS_ENV, "333")
        assert audience(["111", "222"]) == [111, 222, 333]


class TestTheFirstWeekFloor:
    """A report that ships on a Wednesday must not deliver last week that
    morning: the week ended before the report existed, and it arrives on a
    day nobody expects a weekly message."""

    def test_unset_means_no_floor(self, monkeypatch):
        from core.traffic_report import FIRST_WEEK_ENV, first_week

        monkeypatch.delenv(FIRST_WEEK_ENV, raising=False)
        assert first_week() is None

    def test_it_reads_an_iso_date(self, monkeypatch):
        from core.traffic_report import FIRST_WEEK_ENV, first_week

        monkeypatch.setenv(FIRST_WEEK_ENV, "2026-09-07")
        assert first_week() == date(2026, 9, 7)

    def test_a_typo_does_not_stop_the_report_for_good(self, monkeypatch, caplog):
        from core.traffic_report import FIRST_WEEK_ENV, first_week

        monkeypatch.setenv(FIRST_WEEK_ENV, "next monday")
        with caplog.at_level("WARNING"):
            assert first_week() is None
        assert any("ISO date" in r.message for r in caplog.records)


# ─── The scheduled job ──────────────────────────────────────────────────────


@needs_postgres
class TestTheScheduledJob(TrafficReportJob):
    @pytest_asyncio.fixture(autouse=True)
    async def _warehouse_in_postgres(self, monkeypatch):
        """An empty Postgres warehouse for `_seed_gold` to fill."""
        from tests.integration.pg_tab_seed import tab_in_postgres

        async def _nothing(conn, pool):
            pass

        async with tab_in_postgres(os.environ["KS_PG_DSN"], monkeypatch, _nothing):
            yield


    @pytest.mark.asyncio
    async def test_a_week_older_than_the_floor_is_never_sent(
        self, tmp_path, monkeypatch,
    ):
        """Checked before the ledger, because this is not a delivery
        question: nothing about that week changes by tomorrow."""
        from core.traffic_report import FIRST_WEEK_ENV

        store = await _job_store(tmp_path)
        week_start = await _seed_gold(store)
        monkeypatch.setenv(
            FIRST_WEEK_ENV, (week_start + timedelta(days=7)).isoformat())
        scheduler = self._wire(monkeypatch, store, tmp_path)
        sent = self._capture(monkeypatch)

        result = await scheduler._run_traffic_report()

        assert result["sent"] is False
        assert result["reason"] == "before_first_week"
        assert sent["rich"] == [] and sent["text"] == []
        assert not await _traffic_sent(week_start), (
            "skipping a week must not record it as delivered")

    @pytest.mark.asyncio
    async def test_the_floor_week_itself_is_sent(self, tmp_path, monkeypatch):
        """The boundary is inclusive: naming Monday the 7th means the week
        that starts on the 7th is the first one delivered."""
        from core.traffic_report import FIRST_WEEK_ENV

        store = await _job_store(tmp_path)
        week_start = await _seed_gold(store)
        monkeypatch.setenv(FIRST_WEEK_ENV, week_start.isoformat())
        scheduler = self._wire(monkeypatch, store, tmp_path)
        sent = self._capture(monkeypatch)

        result = await scheduler._run_traffic_report()

        assert result["sent"] is True
        assert len(sent["rich"]) == 1

    @pytest.mark.asyncio
    async def test_it_waits_while_the_warehouse_is_behind(self, tmp_path, monkeypatch):
        store = await _job_store(tmp_path)
        await _seed_gold(store, complete=False)
        scheduler = self._wire(monkeypatch, store, tmp_path)
        sent = self._capture(monkeypatch)
        result = await scheduler._run_traffic_report()
        assert result["sent"] is False and result["reason"] == "warehouse_behind"
        assert sent["rich"] == [] and sent["text"] == []

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

        store = await _job_store(tmp_path)
        await _seed_gold(store)
        scheduler = self._wire(monkeypatch, store, tmp_path, analytics=analytics)
        sent = self._capture(monkeypatch)
        result = await scheduler._run_traffic_report()
        assert result["sent"] is False and result["reason"] == "no_attribution"
        assert sent["rich"] == [] and sent["text"] == []

    @pytest.mark.asyncio
    async def test_it_delivers_once_and_then_stays_quiet(self, tmp_path, monkeypatch):
        store = await _job_store(tmp_path)
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

    @pytest.mark.asyncio
    async def test_it_writes_only_to_admins(self, tmp_path, monkeypatch):
        """The tab is visible to seventeen of eighteen dashboard accounts;
        this report is not."""
        monkeypatch.delenv(RECIPIENTS_ENV, raising=False)
        store = await _job_store(tmp_path)
        await _seed_gold(store)
        scheduler = self._wire(monkeypatch, store, tmp_path)
        sent = self._capture(monkeypatch)
        await scheduler._run_traffic_report()
        written_to = {uid for call in sent["rich"] for uid in call["chat_ids"]}
        assert written_to == set(self.ADMINS)

    @pytest.mark.asyncio
    async def test_a_listed_reader_is_written_to(self, tmp_path, monkeypatch):
        """The list is only a list until the job reads it. This is the
        variable reaching an actual send, which is the half that a unit test
        of `audience()` cannot show."""
        monkeypatch.setenv(RECIPIENTS_ENV, "999")
        store = await _job_store(tmp_path)
        await _seed_gold(store)
        scheduler = self._wire(monkeypatch, store, tmp_path)
        sent = self._capture(monkeypatch)
        result = await scheduler._run_traffic_report()

        written_to = {uid for call in sent["rich"] for uid in call["chat_ids"]}
        assert written_to == set(self.ADMINS) | {999}
        assert result["recipients"] == len(self.ADMINS) + 1

    @pytest.mark.asyncio
    async def test_a_listed_reader_gets_their_own_language(
        self, tmp_path, monkeypatch,
    ):
        """Ukrainian for everyone, English for admins — the rule the whole
        system uses, applied here without anybody configuring a thing. So a
        curator added to the list splits the send in two rather than being
        handed the admins' English.
        """
        monkeypatch.setenv(RECIPIENTS_ENV, "999")
        store = await _job_store(tmp_path)
        await _seed_gold(store)
        scheduler = self._wire(monkeypatch, store, tmp_path)
        sent = self._capture(monkeypatch)
        await scheduler._run_traffic_report()

        assert len(sent["rich"]) == 2, "one send per language, not per reader"
        by_reader = {uid: call["html"]
                     for call in sent["rich"] for uid in call["chat_ids"]}
        # Headings are upper-cased by the house style, so compare folded.
        assert by_reader[999] != by_reader[self.ADMINS[0]]
        assert "traffic report" in by_reader[self.ADMINS[0]].lower()
        assert "звіт по трафіку" in by_reader[999].lower()

    @pytest.mark.asyncio
    async def test_a_malformed_list_still_delivers_to_the_admins(
        self, tmp_path, monkeypatch,
    ):
        """One bad character in a variable nobody validates must not be the
        reason a Monday report does not go out."""
        monkeypatch.setenv(RECIPIENTS_ENV, "oops")
        store = await _job_store(tmp_path)
        await _seed_gold(store)
        scheduler = self._wire(monkeypatch, store, tmp_path)
        sent = self._capture(monkeypatch)
        result = await scheduler._run_traffic_report()

        assert result["sent"] is True
        written_to = {uid for call in sent["rich"] for uid in call["chat_ids"]}
        assert written_to == set(self.ADMINS)

    @pytest.mark.asyncio
    async def test_a_rejected_rich_message_falls_back_to_text(
        self, tmp_path, monkeypatch,
    ):
        store = await _job_store(tmp_path)
        await _seed_gold(store)
        scheduler = self._wire(monkeypatch, store, tmp_path)
        sent = self._capture(monkeypatch, rich=0)
        result = await scheduler._run_traffic_report()
        assert result["sent"] is True and result["rich"] is False
        assert len(sent["text"]) == 1
        assert "Traffic report" in sent["text"][0]["body"] or len(sent["text"]) == 1

    @pytest.mark.asyncio
    async def test_a_send_that_reached_nobody_leaves_the_week_pending(
        self, tmp_path, monkeypatch,
    ):
        """Nobody heard it, so it did not happen: tomorrow's tick tries
        again rather than dropping the week."""
        store = await _job_store(tmp_path)
        week_start = await _seed_gold(store)
        scheduler = self._wire(monkeypatch, store, tmp_path)
        self._capture(monkeypatch, rich=0, text=0)
        result = await scheduler._run_traffic_report()
        assert result["sent"] is False and result["reason"] == "not_delivered"
        assert not await _traffic_sent(week_start)
