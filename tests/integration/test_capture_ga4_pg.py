"""The GA4 funnel capture against a real PostgreSQL (revision 0036's `app.ga4_funnel`).

GA4 is `FakeGA4` (`tests/unit/test_capture_ga4.py`), an `httpx.MockTransport`:
nothing reaches Google. The job runs as the scheduler runs it, through
`core.analytics_captures.run_own_transaction` and the real pool's connections.
Every row of `app.ga4_funnel` and the capture's stamp are deleted before and
after: this module is the table's only writer.

What is pinned, each named by the mutation it kills:

- a run keeps the oldest periods, four funnels each, with the rows the page
  would have shown, and stamps the capture;
- a kept period is never asked again and never rewritten;
- the hour's budget is read from the table, so a second run in the hour asks
  nothing;
- a GA4 error stops the run, keeps what was kept, and is stamped as the watch
  reads it (`capture_failed:app.ga4_funnel`, WARN); the next run resumes;
- switched off or without a key, nothing is asked, written or stamped;
- the integrity job counts the overdue periods not kept yet, and never the
  one that fell due today, and judges them by their slack: INFO
  `ga4_backfill_incomplete` with a week of nights to spare, WARN
  `ga4_backlog_at_risk` below it.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import analytics_captures as cap
from core import ga4
from core import ga4_capture as gc
from core.data_quality import Severity
from tests.unit.test_capture_ga4 import TODAY, FakeGA4, ga4_key, people  # noqa: F401 — fixture
from web.services import ga4_funnel_service as svc

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

STAMP = cap.state_row(gc.TABLE)


async def _clean(conn) -> None:
    await conn.execute("DELETE FROM app.ga4_funnel")
    await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = $1", STAMP)


@pytest_asyncio.fixture
async def pool(monkeypatch, ga4_key):  # noqa: F811 — the fixture imported above
    monkeypatch.setenv("KS_PG_DSN", DSN)
    monkeypatch.delenv(cap.SWITCH_ENV, raising=False)
    # The watch reads the backlog as of Kyiv's today; the runs here are TODAY's,
    # and the next run the watch counts nights from is the night after.
    monkeypatch.setattr(gc, "kyiv_today", lambda now=None: TODAY)
    monkeypatch.setattr(gc, "first_night", lambda now: TODAY + timedelta(days=1))
    created = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    async with created.acquire() as conn:
        await _clean(conn)
    with patch("core.pg.get_pool", new=AsyncMock(return_value=created)):
        yield created
    async with created.acquire() as conn:
        await _clean(conn)
    await created.close()


async def _run(google: FakeGA4, today: date = TODAY) -> dict:
    async with google.client() as client:
        return await gc.run_capture(client=client, today=today)


async def _kept(pool) -> list:
    """(grain, period_start) of every period with both scopes' totals, oldest first."""
    rows = await pool.fetch(
        "SELECT grain, period_start FROM app.ga4_funnel "
        "WHERE breakdown = 'total' AND step = 1 "
        "GROUP BY grain, period_start HAVING count(DISTINCT country_scope) = 2 "
        "ORDER BY period_start, array_position(ARRAY['day','week','month'], grain)")
    return [(r["grain"], r["period_start"]) for r in rows]


async def _stamp(pool):
    return await pool.fetchrow(
        "SELECT last_ok_at, failures_since_ok, last_error, last_rows "
        "FROM meta.mirror_state WHERE table_name = $1", STAMP)


async def _an_hour_ago(pool) -> None:
    """Move every kept row's `captured_at` out of the hour's budget."""
    await pool.execute("UPDATE app.ga4_funnel SET captured_at = captured_at - interval '2 hours'")


class TestARun:

    @pytest.mark.asyncio
    async def test_it_keeps_the_oldest_periods_as_the_page_would_show_them(self, pool):
        google = FakeGA4()
        result = await _run(google)
        due = gc.due_periods(TODAY)
        assert result["captured"] == gc.PERIODS_PER_RUN and result["requests"] == 4 * gc.PERIODS_PER_RUN
        assert await _kept(pool) == [(p.grain, p.start) for p in due[:gc.PERIODS_PER_RUN]]

        first = due[0]
        rows = await pool.fetch(
            "SELECT country_scope, breakdown, breakdown_value, traffic_kind, traffic_rule, "
            "step, active_users, period_end FROM app.ga4_funnel "
            "WHERE grain = $1 AND period_start = $2", first.grain, first.start)
        got = {(r["country_scope"], r["breakdown"], r["breakdown_value"], r["step"]):
               (r["active_users"], r["traffic_kind"], r["traffic_rule"]) for r in rows}
        assert {r["period_end"] for r in rows} == {first.end}
        for scope in svc.SCOPES:
            for step, n in enumerate(people(scope, first.start, first.end), start=1):
                assert got[(scope, "total", "", step)] == (n, "", "")
            for channel, share in {"Paid Social": 0.5, "Organic Social": 0.3,
                                   "Direct": 0.15, "Unassigned": 0.05}.items():
                users = people(scope, first.start, first.end, share)[0]
                assert got[(scope, "channel_group", channel, 1)] == (
                    users, svc.channel_type(channel), svc.TRAFFIC_RULE)
            assert got[(scope, "device", "mobile", 1)][0] == people(scope, first.start, first.end, 0.8)[0]
        # 2 scopes x (total + 4 channel groups + 2 devices) x 5 steps.
        assert len(rows) == 2 * 7 * 5
        assert not [k for k in got if k[1] == "device" and k[2] == ""]

        stamp = await _stamp(pool)
        assert stamp["last_ok_at"] is not None and stamp["failures_since_ok"] == 0
        assert stamp["last_rows"] == 70

    @pytest.mark.asyncio
    async def test_the_rows_ks_readonly_reads_are_the_ones_written(self, pool):
        """The table is read by the analysts' role, not only by its writer."""
        await _run(FakeGA4())
        ro = os.getenv("KS_PG_READONLY_DSN")
        if not ro:
            pytest.skip("KS_PG_READONLY_DSN is not set")
        conn = await asyncpg.connect(ro)
        try:
            assert await conn.fetchval("SELECT count(*) FROM app.ga4_funnel") == \
                await pool.fetchval("SELECT count(*) FROM app.ga4_funnel")
        finally:
            await conn.close()


class TestOnce:

    @pytest.mark.asyncio
    async def test_a_kept_period_is_neither_asked_again_nor_rewritten(self, pool):
        await _run(FakeGA4())
        first = gc.due_periods(TODAY)[0]
        await pool.execute(
            "UPDATE app.ga4_funnel SET active_users = 7 WHERE grain = $1 AND period_start = $2",
            first.grain, first.start)
        await _an_hour_ago(pool)

        google = FakeGA4()
        result = await _run(google)
        due = gc.due_periods(TODAY)
        assert google.periods() == [(p.start, p.end) for p in due[24:48]]
        assert result["captured"] == 24
        assert await pool.fetchval(
            "SELECT DISTINCT active_users FROM app.ga4_funnel WHERE grain = $1 AND period_start = $2",
            first.grain, first.start) == 7

    @pytest.mark.asyncio
    async def test_the_insert_does_nothing_on_a_kept_row(self, pool):
        """The race the scheduler's max_instances already prevents: a second
        writer of a kept period inserts nothing and changes nothing."""
        await _run(FakeGA4())
        first = gc.due_periods(TODAY)[0]
        before = await pool.fetchval("SELECT sum(active_users) FROM app.ga4_funnel")
        rows = gc.rows_for("ua", {ga4.TOTAL: dict.fromkeys(ga4.COLUMNS, 1)}, {})
        inserted = [None]
        assert await cap.run_own_transaction(gc.TABLE, gc._keeper(first, rows, inserted))
        assert inserted == [0]
        assert await pool.fetchval("SELECT sum(active_users) FROM app.ga4_funnel") == before


class TestTheThrottle:

    @pytest.mark.asyncio
    async def test_a_second_run_in_the_hour_asks_nothing_and_still_stamps(self, pool):
        await _run(FakeGA4())
        first_ok = (await _stamp(pool))["last_ok_at"]

        google = FakeGA4()
        result = await _run(google)
        assert (result["budget"], result["captured"], google.asked) == (0, 0, [])
        assert (await _stamp(pool))["last_ok_at"] > first_ok
        assert len(await _kept(pool)) == gc.PERIODS_PER_RUN

    @pytest.mark.asyncio
    async def test_what_the_hour_left_is_what_is_asked(self, pool):
        """Four of the first run's periods moved out of the hour: twenty still
        count against it, so four may be asked."""
        await _run(FakeGA4())
        await pool.execute(
            "UPDATE app.ga4_funnel SET captured_at = captured_at - interval '2 hours' "
            "WHERE (grain, period_start) IN (SELECT DISTINCT grain, period_start "
            "FROM app.ga4_funnel ORDER BY period_start, grain LIMIT 4)")
        google = FakeGA4()
        result = await _run(google)
        assert result["budget"] == 4 == result["captured"]
        assert len(google.asked) == 16


class TestFailure:

    @pytest.mark.asyncio
    async def test_a_ga4_error_is_stamped_and_the_next_run_resumes(self, pool):
        google = FakeGA4(fail_at=6)
        result = await _run(google)
        due = gc.due_periods(TODAY)
        assert result["captured"] == 1 and "error" in result
        assert await _kept(pool) == [(due[0].grain, due[0].start)]
        stamp = await _stamp(pool)
        assert stamp["failures_since_ok"] == 1
        assert stamp["last_error"].startswith("GA4Unavailable: GA4 answered 429")

        found = cap.judge(await cap.read_facts(pool=pool))
        mine = [i for i in found if i.table_name == gc.TABLE]
        # One period kept: the next oldest, the day 2025-09-15, leaves GA4's
        # window on the 15th and only the next run can keep it.
        assert [(i.check_name, i.severity) for i in mine] == [
            (f"capture_failed:{gc.TABLE}", Severity.WARN),
            (cap.GA4_AT_RISK, Severity.WARN)]
        assert "only 0 night(s) to spare" in mine[1].description

        await _an_hour_ago(pool)
        retry = FakeGA4()
        result = await _run(retry)
        assert retry.periods()[0] == (due[1].start, due[1].end)
        assert result["captured"] == 24 and "error" not in result
        stamp = await _stamp(pool)
        assert stamp["failures_since_ok"] == 0 and stamp["last_error"] is None
        # The backfill has 25 of its periods; the backlog is the rest, with
        # twenty nights to spare.
        assert [(i.check_name, i.severity)
                for i in cap.judge(await cap.read_facts(pool=pool))
                if i.table_name == gc.TABLE] == [(cap.GA4_BACKFILL, Severity.INFO)]

    @pytest.mark.asyncio
    async def test_a_period_ga4_answers_empty_is_kept_as_zeros(self, pool):
        """What the page would have read. Kills: an empty answer stored as no
        rows at all, which would ask the period again every night."""
        first = gc.due_periods(TODAY)[0]
        await _run(FakeGA4(empty={(first.start, first.end)}))
        rows = await pool.fetch(
            "SELECT breakdown, active_users FROM app.ga4_funnel WHERE grain = $1 AND period_start = $2",
            first.grain, first.start)
        assert sorted((r["breakdown"], r["active_users"]) for r in rows) == [("total", 0)] * 10
        assert (first.grain, first.start) in await _kept(pool)


class TestTheBacklog:

    @pytest.mark.asyncio
    async def test_the_watch_counts_what_is_overdue_and_not_kept(self, pool):
        """Kills: a backlog read off the wrong set — the due periods rather
        than the overdue ones, or ignoring what is kept."""
        await _run(FakeGA4())
        overdue = gc.overdue_periods(TODAY)
        facts = await cap.read_facts(pool=pool)
        assert facts.ga4_backlog is not None
        assert facts.ga4_backlog.periods == tuple(overdue[gc.PERIODS_PER_RUN:])
        assert (facts.ga4_backlog.missing, facts.ga4_backlog.oldest) == (
            len(overdue) - gc.PERIODS_PER_RUN, overdue[gc.PERIODS_PER_RUN])
        assert facts.ga4_backlog.first_night == TODAY + timedelta(days=1)
        (issue,) = [i for i in cap.judge(facts) if i.check_name == cap.GA4_BACKFILL]
        assert (issue.severity, issue.count) == (Severity.INFO, len(overdue) - gc.PERIODS_PER_RUN)
        assert "19 nights to spare" in issue.description

    @pytest.mark.asyncio
    async def test_the_period_that_fell_due_today_is_not_counted(self, pool):
        """It is kept at 02:40 and the 01:00 run must not count it. Kills:
        a backlog filed every night before the capture has run."""
        due = gc.due_periods(TODAY)
        overdue = set(gc.overdue_periods(TODAY))
        fresh = [p for p in due if p not in overdue]
        assert fresh and all(p.end == gc.last_closed_day(TODAY) for p in fresh)
        rows = gc.rows_for("ua", {ga4.TOTAL: dict.fromkeys(ga4.COLUMNS, 1)}, {})
        rows += gc.rows_for("all", {ga4.TOTAL: dict.fromkeys(ga4.COLUMNS, 1)}, {})
        for period in overdue:
            assert await cap.run_own_transaction(gc.TABLE, gc._keeper(period, rows, [0]))
        facts = await cap.read_facts(pool=pool)
        assert facts.ga4_backlog is not None
        assert (facts.ga4_backlog.missing, facts.ga4_backlog.oldest) == (0, None)
        assert not [i for i in cap.judge(facts) if i.table_name == gc.TABLE]

    @pytest.mark.asyncio
    async def test_without_a_key_there_is_no_backlog(self, pool, monkeypatch):
        monkeypatch.delenv("KS_GA4_KEY_FILE", raising=False)
        assert (await cap.read_facts(pool=pool)).ga4_backlog is None


class TestStandingDown:

    @pytest.mark.asyncio
    async def test_switched_off_nothing_is_asked_written_or_stamped(self, pool, monkeypatch):
        monkeypatch.setenv(cap.SWITCH_ENV, "off")
        google = FakeGA4()
        await _run(google)
        assert not google.asked
        assert await pool.fetchval("SELECT count(*) FROM app.ga4_funnel") == 0
        assert await _stamp(pool) is None

    @pytest.mark.asyncio
    async def test_without_a_key_nothing_is_asked_written_or_stamped(self, pool, monkeypatch):
        monkeypatch.delenv("KS_GA4_KEY_FILE", raising=False)
        google = FakeGA4()
        assert await _run(google) == {"skipped": "GA4 is not configured"}
        assert not google.asked
        assert await pool.fetchval("SELECT count(*) FROM app.ga4_funnel") == 0
        assert await _stamp(pool) is None
