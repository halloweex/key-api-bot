"""The weekly report: the window it picks, the arithmetic, and the wording.

Three things here are load-bearing and would fail silently in production:
the delivery window has to be the same all week or a missed Monday drops the
week entirely; the decomposition has to be exact or the two effects will not
add up to the headline; and product names carrying a bare `&` have to be
escaped or Telegram rejects the whole message as unparseable entities.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore
from core.weekly_report import (
    MIN_BASELINE_WEEKS,
    ProductMove,
    WeekTotals,
    WeeklyReport,
    already_sent,
    build_report,
    decompose,
    fetch_product_moves,
    fetch_week_totals,
    fetch_weekly_series,
    format_report,
    last_complete_week,
    mark_sent,
    pct_change,
    same_week_last_year,
    share_of,
    warehouse_max_date,
)


def _report(**overrides) -> WeeklyReport:
    """A report with plausible defaults; override what a test is about."""
    base = dict(
        start=date(2026, 5, 18),
        end=date(2026, 5, 24),
        sales_type="retail",
        current=WeekTotals(revenue=968_638, orders=355,
                           new_customer_orders=170, repeat_orders=185),
        previous=WeekTotals(revenue=1_305_788, orders=478,
                            new_customer_orders=273, repeat_orders=205),
        year_ago=WeekTotals(revenue=603_194, orders=264),
        baseline_mean=1_100_920.0,
        baseline_sd=276_497.0,
        baseline_weeks=12,
        movers=[],
        product_move_total=0.0,
    )
    base.update(overrides)
    return WeeklyReport(**base)


# ─── The window ─────────────────────────────────────────────────────────────

class TestLastCompleteWeek:
    def test_monday_reports_the_week_that_just_ended(self):
        assert last_complete_week(date(2026, 5, 25)) == (
            date(2026, 5, 18), date(2026, 5, 24),
        )

    def test_sunday_does_not_report_the_week_it_is_still_in(self):
        """Sunday is the last day of its own week, not the day after it."""
        assert last_complete_week(date(2026, 5, 24)) == (
            date(2026, 5, 11), date(2026, 5, 17),
        )

    def test_every_day_of_a_week_reports_the_same_window(self):
        """What makes a missed Monday recoverable.

        A weekly trigger that was not alive at its instant computes the next
        fire a week out and the week is never reported. The daily tick works
        only because Tuesday asks for — and gets — Monday's window.
        """
        windows = {
            last_complete_week(date(2026, 5, 25) + timedelta(days=n))
            for n in range(7)
        }
        assert windows == {(date(2026, 5, 18), date(2026, 5, 24))}

    def test_window_is_always_monday_to_sunday(self):
        for day in range(1, 29):
            start, end = last_complete_week(date(2026, 2, day))
            assert start.weekday() == 0
            assert end.weekday() == 6
            assert (end - start).days == 6


class TestSameWeekLastYear:
    def test_aligns_on_weekday_not_calendar_date(self):
        """18 May 2026 is a Monday; 18 May 2025 is a Sunday.

        Calendar alignment would compare a Monday against a Sunday and hand
        back the day-of-week effect as growth.
        """
        start, end = same_week_last_year(date(2026, 5, 18))
        assert start == date(2025, 5, 19)
        assert end == date(2025, 5, 25)
        assert start.weekday() == 0

    def test_week_53_falls_back_instead_of_raising(self):
        """2020 had an ISO week 53; 2019 did not."""
        start, end = same_week_last_year(date(2020, 12, 28))  # 2020-W53
        assert start.weekday() == 0
        assert (end - start).days == 6


# ─── The arithmetic ─────────────────────────────────────────────────────────

class TestDecompose:
    @pytest.mark.parametrize("cur_orders,cur_rev,prev_orders,prev_rev", [
        (355, 968_638, 478, 1_305_788),   # fewer orders, flat basket
        (500, 1_400_000, 478, 1_305_788),  # growth
        (478, 900_000, 478, 1_305_788),    # same orders, smaller basket
        (1, 100, 400, 1_000_000),          # collapse
    ])
    def test_the_two_effects_reconstruct_the_headline(
        self, cur_orders, cur_rev, prev_orders, prev_rev,
    ):
        """No residual, ever — the split is an identity, not an estimate."""
        cur = WeekTotals(revenue=cur_rev, orders=cur_orders)
        prev = WeekTotals(revenue=prev_rev, orders=prev_orders)
        orders_effect, check_effect = decompose(cur, prev)
        assert orders_effect + check_effect == pytest.approx(cur_rev - prev_rev)

    def test_a_pure_volume_drop_lands_entirely_on_order_count(self):
        cur = WeekTotals(revenue=500_000, orders=200)
        prev = WeekTotals(revenue=750_000, orders=300)  # identical ₴2,500 basket
        orders_effect, check_effect = decompose(cur, prev)
        assert orders_effect == pytest.approx(-250_000)
        assert check_effect == pytest.approx(0)


class TestPctChange:
    def test_no_base_means_no_percentage(self):
        assert pct_change(100, 0) is None
        assert pct_change(100, None) is None

    def test_ordinary_change(self):
        assert pct_change(968_638, 1_305_788) == pytest.approx(-25.81, abs=0.01)


class TestShareOf:
    def test_zero_whole_has_no_share(self):
        assert share_of(50, 0) is None

    def test_a_part_pointing_the_other_way_has_no_share(self):
        """A gainer inside a losing week is not '−30% of the drop'."""
        assert share_of(30_000, -100_000) is None

    def test_an_overshooting_share_is_withheld(self):
        """A week whose gains and losses nearly cancel has a near-zero total.

        "This product is 4,000% of the move" is arithmetically true and says
        nothing, so it is not said.
        """
        assert share_of(80_000, 2_000) is None

    def test_ordinary_share(self):
        assert share_of(-158_075, -339_041) == pytest.approx(46.6, abs=0.1)


class TestAnomalyGate:
    def test_a_quarter_lost_can_still_be_an_ordinary_week(self):
        """The whole reason the gate exists.

        Weekly revenue swings with σ ≈ ₴276K around ₴1.1M. Calling every
        25% move an event trains people to stop reading the report.
        """
        z = _report().z
        assert z == pytest.approx(-0.48, abs=0.01)
        assert abs(z) < 1.5

    def test_too_little_history_says_nothing_rather_than_guessing(self):
        assert _report(baseline_weeks=MIN_BASELINE_WEEKS - 1).z is None

    def test_a_flat_history_has_no_spread_to_measure(self):
        assert _report(baseline_sd=0.0).z is None


# ─── The wording ────────────────────────────────────────────────────────────

class TestFormatting:
    def test_an_ampersand_in_a_product_name_is_escaped(self):
        """Telegram rejects the entire message on unparseable entities.

        The catalogue is full of "Differ & Deeper …", so this is the
        difference between a weekly report and weekly silence.
        """
        text = format_report(_report(
            movers=[ProductMove(name="Differ & Deeper Cream", current=1.0, previous=2.0)],
            product_move_total=-1.0,
        ))
        assert "Differ &amp; Deeper" in text
        assert "Differ & Deeper" not in text

    def test_a_tenth_of_a_percent_reads_as_flat(self):
        """₴2,729 against ₴2,732 is not a movement anyone should react to."""
        text = format_report(_report())
        assert "≈ flat" in text

    def test_it_names_the_lever_that_actually_moved(self):
        text = format_report(_report())
        assert "order count, not basket size" in text

    def test_an_unusual_week_is_marked_unusual(self):
        text = format_report(_report(current=WeekTotals(revenue=2_000_000, orders=700)))
        assert "Unusually high" in text
        assert "Inside the normal range" not in text

    def test_a_first_week_with_no_history_still_renders(self):
        text = format_report(_report(
            previous=None, year_ago=None,
            baseline_mean=None, baseline_sd=None, baseline_weeks=0,
        ))
        assert "₴ 968,638" in text
        assert "What moved" not in text

    def test_the_dashboard_link_is_optional(self):
        assert "<a href" not in format_report(_report())
        assert "<a href" in format_report(_report(), "https://example.org")


# ─── The reads ──────────────────────────────────────────────────────────────

async def _store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "weekly.duckdb")
    await s.connect()
    return s


def _gold_day(conn, day: date, revenue: float, orders: int, sales_type="retail"):
    conn.execute(
        "INSERT OR REPLACE INTO gold_daily_revenue "
        "(date, sales_type, revenue, orders_count) VALUES (?, ?, ?, ?)",
        [day, sales_type, revenue, orders],
    )


def _silver_order(conn, oid: int, day: date, total: float, is_new: bool,
                  sales_type="retail", is_return=False):
    conn.execute(
        "INSERT OR REPLACE INTO silver_orders "
        "(id, source_id, status_id, grand_total, order_date, is_return, "
        " sales_type, is_active_source, source_name, is_new_customer) "
        "VALUES (?, 1, 1, ?, ?, ?, ?, TRUE, 'Instagram', ?)",
        [oid, total, day, is_return, sales_type, is_new],
    )


def _gold_product(conn, day: date, pid: int, name: str, revenue: float,
                  sales_type="retail"):
    conn.execute(
        "INSERT INTO gold_daily_products "
        "(date, sales_type, source_id, product_id, product_name, "
        " quantity_sold, product_revenue) VALUES (?, ?, 1, ?, ?, 1, ?)",
        [day, sales_type, pid, name, revenue],
    )


class TestWeekTotals:
    @pytest.mark.asyncio
    async def test_orders_split_exactly_into_new_and_repeat(self, tmp_path):
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                _gold_day(conn, date(2026, 5, 18), 3_000, 3)
                _silver_order(conn, 1, date(2026, 5, 18), 1_000, is_new=True)
                _silver_order(conn, 2, date(2026, 5, 19), 1_000, is_new=False)
                _silver_order(conn, 3, date(2026, 5, 20), 1_000, is_new=False)
                # Outside the window, and a return inside it — neither counts.
                _silver_order(conn, 4, date(2026, 5, 26), 9_000, is_new=True)
                _silver_order(conn, 5, date(2026, 5, 21), 9_000, is_new=True,
                              is_return=True)

                totals = fetch_week_totals(
                    conn, date(2026, 5, 18), date(2026, 5, 24), "retail",
                )
            assert totals.new_customer_orders == 1
            assert totals.repeat_orders == 2
            assert (totals.new_customer_orders + totals.repeat_orders
                    == totals.orders == 3)
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_revenue_comes_from_gold_so_it_matches_the_dashboard(self, tmp_path):
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                _gold_day(conn, date(2026, 5, 18), 100_000, 40)
                _gold_day(conn, date(2026, 5, 19), 50_000, 10)
                _gold_day(conn, date(2026, 5, 26), 999_999, 1)   # next week
                _gold_day(conn, date(2026, 5, 20), 777_777, 7, sales_type="b2b")

                totals = fetch_week_totals(
                    conn, date(2026, 5, 18), date(2026, 5, 24), "retail",
                )
            assert totals.revenue == 150_000
            assert totals.orders == 50
            assert totals.avg_check == 3_000
        finally:
            await store.close()


class TestProductMoves:
    @pytest.mark.asyncio
    async def test_ranked_by_hryvnia_not_percent(self, tmp_path):
        """The design decision the whole block rests on.

        Percent ranking floats ₴500 → ₴2,500 (+400%) to the top and buries
        ₴101,885 → ₴19,966, which by itself was a quarter of that week's
        entire decline.
        """
        store = await _store(tmp_path)
        try:
            cur, prev = date(2026, 5, 18), date(2026, 5, 11)
            async with store.connection() as conn:
                _gold_product(conn, prev, 1, "Small but exciting", 500)
                _gold_product(conn, cur, 1, "Small but exciting", 2_500)
                _gold_product(conn, prev, 2, "The one that mattered", 101_885)
                _gold_product(conn, cur, 2, "The one that mattered", 19_966)

                moves, total = fetch_product_moves(
                    conn, cur, cur + timedelta(days=6),
                    prev, prev + timedelta(days=6),
                    "retail",
                )
            assert moves[0].name == "The one that mattered"
            assert moves[0].delta == pytest.approx(-81_919)
            assert total == pytest.approx(-79_919)
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_product_that_only_sold_in_one_week_still_appears(self, tmp_path):
        """A launch and a stockout are both a full-value move, not a missing row."""
        store = await _store(tmp_path)
        try:
            cur, prev = date(2026, 5, 18), date(2026, 5, 11)
            async with store.connection() as conn:
                _gold_product(conn, cur, 1, "Launched this week", 11_130)
                _gold_product(conn, prev, 2, "Sold out", 8_000)

                moves, _ = fetch_product_moves(
                    conn, cur, cur + timedelta(days=6),
                    prev, prev + timedelta(days=6),
                    "retail",
                )
            by_name = {m.name: m.delta for m in moves}
            assert by_name["Launched this week"] == pytest.approx(11_130)
            assert by_name["Sold out"] == pytest.approx(-8_000)
        finally:
            await store.close()


class TestBaselineSeries:
    @pytest.mark.asyncio
    async def test_a_dead_week_inside_the_record_counts_as_zero(self, tmp_path):
        """Dropping it would flatter the average and hide the drought."""
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                for week in range(4):
                    day = date(2026, 4, 20) + timedelta(days=7 * week)
                    if week == 2:
                        continue  # no orders at all that week
                    _gold_day(conn, day, 100_000, 40)

                series = fetch_weekly_series(
                    conn, date(2026, 5, 18), "retail", weeks=4,
                )
            assert series == [100_000, 100_000, 0.0, 100_000]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_weeks_before_the_first_record_are_not_invented(self, tmp_path):
        """History that predates the data is not a run of zero-revenue weeks."""
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                _gold_day(conn, date(2026, 5, 11), 100_000, 40)
                series = fetch_weekly_series(
                    conn, date(2026, 5, 18), "retail", weeks=12,
                )
            assert series == [100_000]
        finally:
            await store.close()


class TestSendLedger:
    @pytest.mark.asyncio
    async def test_a_week_is_delivered_once(self, tmp_path):
        store = await _store(tmp_path)
        try:
            week = date(2026, 5, 18)
            async with store.connection() as conn:
                assert already_sent(conn, week, "retail") is False
                mark_sent(conn, week, "retail", 968_638.0, 355)
                assert already_sent(conn, week, "retail") is True
                # A different week, and a different book, are still pending.
                assert already_sent(conn, date(2026, 5, 25), "retail") is False
                assert already_sent(conn, week, "b2b") is False
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_recording_twice_does_not_raise(self, tmp_path):
        """A retry after a partial failure must not poison the ledger."""
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                mark_sent(conn, date(2026, 5, 18), "retail", 1.0, 1)
                mark_sent(conn, date(2026, 5, 18), "retail", 2.0, 2)
                row = conn.execute(
                    "SELECT revenue, orders FROM weekly_report_sends"
                ).fetchall()
            assert row == [(pytest.approx(2.0), 2)]
        finally:
            await store.close()


class TestReadinessGate:
    @pytest.mark.asyncio
    async def test_the_gate_is_the_warehouse_date_not_this_type_s_row_count(
        self, tmp_path,
    ):
        """b2b runs nine orders a week and legitimately has empty days.

        Asking for seven rows of *this* sales type would defer its report
        forever; asking whether the warehouse has moved past the week end is
        the same question without that trap.
        """
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                assert warehouse_max_date(conn) is None
                _gold_day(conn, date(2026, 5, 20), 50_000, 2, sales_type="b2b")
                _gold_day(conn, date(2026, 5, 24), 60_000, 3, sales_type="retail")
                assert warehouse_max_date(conn) == date(2026, 5, 24)
        finally:
            await store.close()


class TestSchedulerJob:
    """The three gates, exercised through the job the scheduler actually runs."""

    async def _seed(self, store, *, complete: bool):
        """A full baseline plus the reported week, from the real clock's view."""
        from datetime import datetime
        from core.scheduler import SCHEDULER_TIMEZONE

        today = datetime.now(SCHEDULER_TIMEZONE).date()
        start, end = last_complete_week(today)
        last_day = end if complete else end - timedelta(days=3)

        async with store.connection() as conn:
            for n in range(70):
                day = start - timedelta(days=70 - n)
                _gold_day(conn, day, 10_000 + (n % 5) * 500, 4)
            day = start
            while day <= last_day:
                _gold_day(conn, day, 5_000, 2)
                _silver_order(conn, 900 + day.toordinal() % 1000, day, 2_500,
                              is_new=True)
                day += timedelta(days=1)
        return start

    ADMINS = [111, 222]

    def _wire(self, monkeypatch, store, sent, tmp_path=None, languages=None,
              approved=None):
        """Point the job at this store, these admins, and a capturing transport.

        `languages` and `approved` are written into a throwaway SQLite standing
        in for the bot's — without it the job would read the developer's real
        `data/bot.db` and the test would pass or fail by whatever happens to be
        stored there.
        """
        import importlib
        import sqlite3

        from core.scheduler import BackgroundScheduler

        # `core/__init__.py` re-exports the AppConfig instance as `config`, so
        # a dotted monkeypatch target resolves `core.config` to that object
        # rather than to the module. Only an explicit lookup gets the module.
        core_config = importlib.import_module("core.config")

        async def _get_store():
            return store

        async def _send_text(text, **kwargs):
            sent.append(text)
            return len(kwargs.get("chat_ids") or self.ADMINS)

        db = (tmp_path or store.db_path.parent) / "prefs.db"
        with sqlite3.connect(db) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS user_preferences "
                         "(user_id INTEGER PRIMARY KEY, language TEXT, "
                         " notifications_enabled INTEGER DEFAULT 1)")
            conn.executemany(
                "INSERT OR REPLACE INTO user_preferences (user_id, language) "
                "VALUES (?, ?)", list((languages or {}).items()))
            conn.execute("CREATE TABLE IF NOT EXISTS authorized_users "
                         "(user_id INTEGER PRIMARY KEY, status TEXT)")
            conn.executemany("INSERT OR REPLACE INTO authorized_users VALUES (?, ?)",
                             [(uid, "approved") for uid in (approved or [])])

        monkeypatch.setattr("core.bot_prefs.BOT_DB_PATH", db)
        monkeypatch.setattr(core_config, "ADMIN_USER_IDS", self.ADMINS)
        monkeypatch.setattr("core.duckdb_store.get_store", _get_store)
        monkeypatch.setattr("core.telegram_alerts.send_admin_message_http", _send_text)
        return BackgroundScheduler()

    @pytest.mark.asyncio
    async def test_it_waits_while_the_warehouse_is_behind(self, tmp_path, monkeypatch):
        """Numbers rendered mid-rebuild would understate revenue with confidence."""
        store = await _store(tmp_path)
        try:
            await self._seed(store, complete=False)
            sent = []
            result = await self._wire(monkeypatch, store, sent, tmp_path)._run_weekly_report()

            assert result == {"sent": False, "reason": "warehouse_behind",
                              "week": result["week"]}
            assert sent == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_it_delivers_once_and_then_stays_quiet(self, tmp_path, monkeypatch):
        """Six of seven daily firings must say nothing."""
        store = await _store(tmp_path)
        try:
            week_start = await self._seed(store, complete=True)
            sent = []
            scheduler = self._wire(monkeypatch, store, sent, tmp_path)

            first = await scheduler._run_weekly_report()
            assert first["sent"] is True
            assert first["week"] == week_start.isoformat()
            assert first["revenue"] == 35_000
            assert first["delivered"] == len(self.ADMINS)
            assert len(sent) == 1
            assert "Weekly report" in sent[0]

            second = await scheduler._run_weekly_report()
            assert second == {"sent": False, "reason": "already_sent",
                              "week": week_start.isoformat()}
            assert len(sent) == 1
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_it_prefers_the_card_with_the_report_as_its_caption(
        self, tmp_path, monkeypatch,
    ):
        """One message carrying both, not a picture and then a wall of text."""
        store = await _store(tmp_path)
        try:
            await self._seed(store, complete=True)
            text_sends = []
            scheduler = self._wire(monkeypatch, store, text_sends, tmp_path)

            calls = []

            async def _photo(data, caption="", **kwargs):
                calls.append((data, caption))
                return 2

            monkeypatch.setattr(
                "core.telegram_alerts.send_admin_photo_http", _photo,
            )
            result = await scheduler._run_weekly_report()

            assert result["card"] is True
            assert len(calls) == 1
            png, caption = calls[0]
            assert png[:8] == b"\x89PNG\r\n\x1a\n"
            assert "Weekly report" in caption
            assert text_sends == [], "the caption already carried the report"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_each_admin_is_written_to_in_their_own_language(
        self, tmp_path, monkeypatch,
    ):
        """Two languages among the admins means two renders, not one guess."""
        store = await _store(tmp_path)
        try:
            await self._seed(store, complete=True)
            scheduler = self._wire(monkeypatch, store, [], tmp_path,
                                   languages={111: "uk", 222: "en"})

            calls = []

            async def _photo(data, caption="", chat_ids=None, **kwargs):
                calls.append((caption, list(chat_ids or [])))
                return len(chat_ids or [])

            monkeypatch.setattr("core.telegram_alerts.send_admin_photo_http", _photo)
            result = await scheduler._run_weekly_report()

            assert result["delivered"] == 2
            by_recipient = {tuple(ids): caption for caption, ids in calls}
            assert by_recipient[(111,)].startswith("📊 <b>Тижневий звіт</b>")
            assert by_recipient[(222,)].startswith("📊 <b>Weekly report</b>")
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_it_reaches_staff_in_ukrainian_and_admins_in_english(
        self, tmp_path, monkeypatch,
    ):
        """The default policy, end to end, plus a stored choice overriding it."""
        store = await _store(tmp_path)
        try:
            await self._seed(store, complete=True)
            scheduler = self._wire(
                monkeypatch, store, [], tmp_path,
                approved=[301, 302, 303],
                languages={303: "ru"},   # chose Russian; the default must yield
            )

            calls = []

            async def _photo(data, caption="", chat_ids=None, **kwargs):
                calls.append((caption.split("\n")[0], sorted(chat_ids or [])))
                return len(chat_ids or [])

            monkeypatch.setattr("core.telegram_alerts.send_admin_photo_http", _photo)
            result = await scheduler._run_weekly_report()

            # Two admins plus three approved staff, nobody twice.
            assert result["recipients"] == 5
            assert result["delivered"] == 5

            by_head = {head: ids for head, ids in calls}
            assert by_head["📊 <b>Weekly report</b>"] == self.ADMINS
            assert by_head["📊 <b>Тижневий звіт</b>"] == [301, 302]
            assert by_head["📊 <b>Недельный отчёт</b>"] == [303]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_someone_who_muted_notifications_is_not_written_to(
        self, tmp_path, monkeypatch,
    ):
        """A toggle some messages ignore is worse than no toggle at all."""
        import sqlite3

        store = await _store(tmp_path)
        try:
            await self._seed(store, complete=True)
            scheduler = self._wire(monkeypatch, store, [], tmp_path,
                                   approved=[301, 302])
            with sqlite3.connect(tmp_path / "prefs.db") as conn:
                conn.execute("INSERT OR REPLACE INTO user_preferences "
                             "(user_id, notifications_enabled) VALUES (302, 0)")

            reached = []

            async def _photo(data, caption="", chat_ids=None, **kwargs):
                reached.extend(chat_ids or [])
                return len(chat_ids or [])

            monkeypatch.setattr("core.telegram_alerts.send_admin_photo_http", _photo)
            await scheduler._run_weekly_report()

            assert 301 in reached
            assert 302 not in reached
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_an_unreadable_preferences_db_still_delivers_in_english(
        self, tmp_path, monkeypatch,
    ):
        """The bot's SQLite may be locked, missing, or one deploy behind.

        None of that is a reason for the week to go unreported.
        """
        store = await _store(tmp_path)
        try:
            await self._seed(store, complete=True)
            sent = []
            scheduler = self._wire(monkeypatch, store, sent, tmp_path)
            monkeypatch.setattr("core.bot_prefs.BOT_DB_PATH",
                                tmp_path / "does-not-exist.db")

            result = await scheduler._run_weekly_report()

            assert result["sent"] is True
            assert len(sent) == 1
            assert "Weekly report" in sent[0]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_card_that_will_not_send_falls_back_to_text(
        self, tmp_path, monkeypatch,
    ):
        """A missing font must cost the picture and never the numbers."""
        store = await _store(tmp_path)
        try:
            await self._seed(store, complete=True)
            text_sends = []
            scheduler = self._wire(monkeypatch, store, text_sends, tmp_path)
            monkeypatch.setattr("core.weekly_report_image._font_file",
                                lambda *a, **k: None)

            result = await scheduler._run_weekly_report()

            assert result["sent"] is True
            assert result["card"] is False
            assert len(text_sends) == 1
            assert "Weekly report" in text_sends[0]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_failed_send_leaves_the_week_pending(self, tmp_path, monkeypatch):
        """Marking a week delivered that never left the host would drop it."""
        store = await _store(tmp_path)
        try:
            week_start = await self._seed(store, complete=True)
            scheduler = self._wire(monkeypatch, store, [], tmp_path)

            async def _reaches_nobody(*args, **kwargs):
                return 0

            for name in ("send_admin_photo_http", "send_admin_message_http"):
                monkeypatch.setattr(f"core.telegram_alerts.{name}", _reaches_nobody)

            result = await scheduler._run_weekly_report()
            assert result == {"sent": False, "reason": "not_delivered",
                              "week": week_start.isoformat()}

            async with store.connection() as conn:
                assert already_sent(conn, week_start, "retail") is False
        finally:
            await store.close()


class TestBuildReport:
    @pytest.mark.asyncio
    async def test_end_to_end_on_a_small_warehouse(self, tmp_path):
        store = await _store(tmp_path)
        try:
            async with store.connection() as conn:
                # Eight baseline weeks at ₴70K, two of them off by ±₴14K so the
                # spread is real — an unvarying history has no σ and the gate
                # would correctly decline to judge.
                for n in range(56):
                    day = date(2026, 3, 23) + timedelta(days=n)
                    week = n // 7
                    daily = {0: 12_000, 1: 8_000}.get(week, 10_000)
                    _gold_day(conn, day, daily, 2)
                # The reported week: half the revenue at an unchanged ₴5,000
                # basket, so the whole move has to land on order count.
                for n in range(7):
                    day = date(2026, 5, 18) + timedelta(days=n)
                    _gold_day(conn, day, 5_000, 1)
                    _silver_order(conn, 100 + n, day, 5_000, is_new=(n < 3))

                report = build_report(conn, date(2026, 5, 25), "retail")

            assert (report.start, report.end) == (date(2026, 5, 18), date(2026, 5, 24))
            assert (report.current.revenue, report.current.orders) == (35_000, 7)
            assert (report.previous.revenue, report.previous.orders) == (70_000, 14)
            assert report.current.new_customer_orders == 3
            assert report.current.repeat_orders == 4
            assert report.baseline_weeks == 8
            assert report.z is not None and report.z < -1.5

            text = format_report(report)
            assert "▼ 50.0%" in text
            assert "Unusually low" in text
            assert "order count, not basket size" in text
        finally:
            await store.close()
