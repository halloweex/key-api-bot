"""DN-20c against a real PostgreSQL: a consumer reads a working Postgres, and
answers a real outage itself.

`tests/unit/test_read_fallback_off_consumers.py` shows each answer with a
pool that raises on first touch. What it cannot show is the half that would
cost the business: that the Monday traffic report, the goals job and the
assistant simply *read* a Postgres that answers. An answer wired one step too
early — a routing decision or an empty week taken for a refusal — would pass
every unit test and stop both reports, with a deferral in the log that looks
exactly like the designed one.

So the three consumers whose reads go through a router that refuses are run
here against a live Postgres at the current head: the traffic report goes
out to the admins with the week's numbers and records the week, the goals
job writes both tables, the tool answers. Then the same pool is closed — an
outage asyncpg reports on its own — and each answers by name: the report
defers with no ledger row, the goals job defers with nothing written, the
tool returns `data_unavailable`. The consumers with no fallback at all
(the weekly report, training, the index, the buyers step) meet a real
outage exactly as before DN-20c and are not repeated here.

Until step 14's S14-16 the fixture set `KS_READ_FALLBACK=off` and each test
held that nothing fell back to DuckDB; nothing reads that variable since,
and nothing counts such a read.
"""
from __future__ import annotations

import os
from unittest.mock import AsyncMock, patch

import asyncpg
import pytest
import pytest_asyncio

from core import read_fallback

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

SWITCHES = {
    "KS_READ_TRAFFIC": "postgres",
    "KS_READ_GOALS": "postgres",
    "KS_READ_EXPENSES": "postgres",
}


@pytest_asyncio.fixture
async def live(monkeypatch):
    """Every read switch unset but the three consumers' routers (none of
    which is read since step 14's S14-07), and a live Postgres behind
    `core.pg.get_pool` that the test can count and close."""
    from core import pg_expenses_read

    for name in [n for n in os.environ if n.startswith("KS_READ_")]:
        monkeypatch.delenv(name, raising=False)
    for name in ("KS_WRITE_GOALS", "KS_WRITE_EXPENSES", "KS_WRITE_INVENTORY"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("KS_PG_DSN", DSN)
    for name, value in SWITCHES.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(pg_expenses_read, "_backfilled", False)
    monkeypatch.setattr(pg_expenses_read, "_checked_at", 0.0)
    monkeypatch.setattr(pg_expenses_read, "_check_failed", None)

    read_fallback.reset_counts()

    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    get_pool = AsyncMock(return_value=pool)
    try:
        with patch("core.pg.get_pool", new=get_pool):
            yield pool, get_pool
    finally:
        if not pool._closed:
            await pool.close()
        read_fallback.reset_counts()


def _refused(surface: str) -> dict:
    return {"reason": read_fallback.READ_UNAVAILABLE, "surface": surface}


def _the_reported_week():
    """Monday–Sunday of the last complete week in Kyiv: the week the job
    reports today."""
    from datetime import datetime

    from core.scheduler import SCHEDULER_TIMEZONE
    from core.weekly_report import last_complete_week

    return last_complete_week(datetime.now(SCHEDULER_TIMEZONE).date())


def _midday(day):
    from datetime import datetime, time
    from zoneinfo import ZoneInfo

    return datetime.combine(day, time(12), ZoneInfo("Europe/Kyiv"))


async def _a_week_of_attributed_orders(conn, pool):
    """Three retail orders in the reported week, the last on its Sunday so
    the warehouse has reached the week's end, and one the week before; two
    of them tagged as an ad, one placed as organic, and one with no tags
    from Instagram, which the tab reads as organic. Bronze landed, Silver and
    Gold derived, then the attribution rows the parse writes."""
    from datetime import timedelta

    from tests.integration.pg_tab_seed import derive, seed

    start, end = _the_reported_week()
    # (order_id, source_id, grand_total, day)
    orders = [(1, 4, 1000.0, start), (2, 4, 500.0, start + timedelta(days=2)),
              (3, 1, 300.0, end), (4, 4, 800.0, start - timedelta(days=6))]
    await seed(conn, orders=[(oid, source, 1, total, _midday(day), oid, None, None)
                             for oid, source, total, day in orders])
    await derive(pool)
    await conn.executemany(
        "INSERT INTO silver.order_utm (order_id, utm_source, utm_medium, utm_campaign,"
        " traffic_type, platform, parsed_at) VALUES ($1, $2, $3, $4, $5, $6, now())",
        [(1, "fbads", "paid", "spring", "paid_confirmed", "facebook"),
         (2, "instagram", "bio", None, "organic", "instagram"),
         (4, "fbads", "paid", "spring", "paid_confirmed", "facebook")])


_SENDS = "FROM app.traffic_report_sends WHERE week_start = $1 AND sales_type = 'retail'"
_OWNER = "FROM meta.chain_watermarks WHERE key = 'owner:app.traffic_report_sends'"


@pytest_asyncio.fixture
async def traffic_ledger_left_as_found():
    """The reported week unsent in chain 11b's ledger, and the ledger and its
    owner row as they were once the test is over."""
    start, _end = _the_reported_week()
    conn = await asyncpg.connect(DSN)
    try:
        sends = await conn.fetch(f"SELECT * {_SENDS}", start)
        owner = await conn.fetch(f"SELECT * {_OWNER}")
        await conn.execute(f"DELETE {_SENDS}", start)
    finally:
        await conn.close()
    yield start
    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute(f"DELETE {_SENDS}", start)
        for row in sends:
            await conn.execute(
                "INSERT INTO app.traffic_report_sends (week_start, sales_type, revenue,"
                " orders, sent_at) VALUES ($1, $2, $3, $4, $5)",
                row["week_start"], row["sales_type"], row["revenue"], row["orders"],
                row["sent_at"])
        if not owner:
            await conn.execute(f"DELETE {_OWNER}")
    finally:
        await conn.close()


def _deliveries(monkeypatch) -> list:
    """Telegram, at the two transports the report sends through: each
    message's form and recipient, every recipient reached."""
    got: list = []

    async def _rich(html, media=None, chat_ids=None, **kwargs):
        got.extend(("rich", chat_id) for chat_id in chat_ids or [])
        return len(chat_ids or [])

    async def _text(body, chat_ids=None, **kwargs):
        got.extend(("text", chat_id) for chat_id in chat_ids or [])
        return len(chat_ids or [])

    monkeypatch.setattr("core.telegram_alerts.send_rich_message_http", _rich)
    monkeypatch.setattr("core.telegram_alerts.send_admin_message_http", _text)
    return got


class TestTheTrafficReport:
    """The readiness gate and the tab's reads are Postgres' since step 14's
    S14-07: seeded there for the working case, and the gate answered for the
    outage, whose refusal under test is the tab's own read (the gate's
    failure is no refusal)."""

    async def _job(self, monkeypatch, tmp_path):
        from tests.helpers.off_consumers import TrafficReportWiring
        from tests.helpers.off_consumers import _duck

        store = await _duck(tmp_path)
        week_start = TrafficReportWiring._the_warehouse_is_ready(monkeypatch)
        scheduler, sent = TrafficReportWiring()._wire(monkeypatch, store, tmp_path)
        return store, week_start, scheduler, sent

    @pytest.mark.asyncio
    async def test_a_week_of_attributed_orders_is_sent_to_the_admins(
        self, monkeypatch, tmp_path, traffic_ledger_left_as_found,
    ):
        """The report reads the tab's Postgres and goes out: the week's three
        orders and ₴1 800, one message to each admin, the week recorded in
        the ledger."""
        from core.store import AnalyticsStore
        from tests.helpers.off_consumers import TrafficReportWiring
        from tests.integration.pg_tab_seed import tab_in_postgres

        week_start = traffic_ledger_left_as_found
        async with tab_in_postgres(DSN, monkeypatch, _a_week_of_attributed_orders) as pool:
            scheduler, _ = TrafficReportWiring()._wire(monkeypatch, AnalyticsStore(), tmp_path)
            delivered = _deliveries(monkeypatch)
            result = await scheduler._run_traffic_report()
            async with pool.acquire() as conn:
                recorded = await conn.fetchrow(f"SELECT orders, revenue {_SENDS}", week_start)

        assert result["sent"] is True, result
        assert result["week"] == week_start.isoformat()
        assert (result["orders"], result["revenue"]) == (3, 1800.0)
        assert sorted(chat_id for _form, chat_id in delivered) == sorted(
            TrafficReportWiring.ADMINS)
        assert recorded is not None, "the delivered week was not recorded"
        assert (recorded["orders"], float(recorded["revenue"])) == (3, 1800.0)

    @pytest.mark.asyncio
    async def test_an_outage_defers_it_with_no_ledger_row(
        self, live, monkeypatch, tmp_path,
    ):
        from tests.helpers.off_consumers import _ledger_answers

        pool, _get_pool = live
        store, week_start, scheduler, sent = await self._job(monkeypatch, tmp_path)
        # The ledger is chain 11b's, in Postgres since step 14's S14-00,
        # and asked first: answered here, so the refusal under test is the
        # report's own read rather than the ledger's.
        marked = _ledger_answers(monkeypatch)
        await pool.close()

        result = await scheduler._run_traffic_report()

        assert result == {"sent": False, "week": week_start.isoformat(),
                          **_refused("traffic")}
        assert sent == []
        marked.assert_not_awaited()


async def _twenty_five_march_days(conn, pool):
    """Twenty-five retail days in one month, landed in Bronze, Silver derived
    by Postgres: enough for the seasonality pass to write a row, so "both
    tables are written" is a claim about rows that would otherwise not be
    there."""
    from datetime import datetime, timezone

    from tests.integration.pg_tab_seed import derive, seed

    await seed(conn, orders=[
        (n + 1, 1, 1, 1000.0, datetime(2025, 3, 1 + n, 12, tzinfo=timezone.utc), 9,
         None, None) for n in range(25)])
    await derive(pool)


async def _goal_tables(pool):
    async with pool.acquire() as conn:
        return (await conn.fetchval("SELECT COUNT(*) FROM app.seasonal_indices"),
                await conn.fetchval("SELECT COUNT(*) FROM app.growth_metrics"))


class TestTheGoalsJob:
    """The history is Postgres' Silver and the goal tables are Postgres'
    (chain 7b-3, a constant since S14-00) — production's arrangement, which
    until step 14's S14-07 this held on DuckDB's goal tables."""

    @pytest.mark.asyncio
    async def test_a_working_postgres_is_read_and_both_tables_are_written(
        self, live, monkeypatch, tmp_path,
    ):
        from core.scheduler import BackgroundScheduler
        from tests.integration.pg_tab_seed import (
            store_that_never_reaches_duckdb, tab_in_postgres,
        )
        from tests.helpers.off_consumers import _serve

        _pool, get_pool = live
        async with tab_in_postgres(DSN, monkeypatch, _twenty_five_march_days) as pool:
            # `tab_in_postgres` hands `core.pg.get_pool` its own pool; the
            # count is asked of the live fixture's, so give that one back.
            get_pool.return_value = pool
            with patch("core.pg.get_pool", new=get_pool):
                store = store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the goals job")
                _serve(monkeypatch, store)
                result = await BackgroundScheduler()._run_seasonality_calc()

                assert get_pool.await_count > 0
                assert "skipped" not in result, result
                assert result["retail_months"] == 1, result
                assert await _goal_tables(pool) == (1, 1)
                assert read_fallback.refusals() == {}

    @pytest.mark.asyncio
    async def test_an_outage_defers_it_with_nothing_written(
        self, live, monkeypatch, tmp_path,
    ):
        from core.scheduler import BackgroundScheduler
        from tests.integration.pg_tab_seed import store_that_never_reaches_duckdb
        from tests.helpers.off_consumers import _serve, _writer_recorded

        pool, _get_pool = live
        store = store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the goals job")
        _serve(monkeypatch, store)
        written = _writer_recorded(monkeypatch)
        await pool.close()

        result = await BackgroundScheduler()._run_seasonality_calc()

        assert result == {"skipped": True, **_refused("goals")}
        assert written == []


class TestTheAssistant:
    @pytest.mark.asyncio
    async def test_a_working_postgres_answers_the_tool(self, live, tmp_path, monkeypatch):
        from core.chat_tools import execute_tool
        from tests.helpers.off_consumers import AssistantTools
        from tests.helpers.off_consumers import _duck

        _pool, get_pool = live
        store = await _duck(tmp_path)
        AssistantTools._serve_tools(monkeypatch, store)
        result = await execute_tool("list_expenses", {})

        assert get_pool.await_count > 0
        assert "error" not in result, result
        assert read_fallback.refusals() == {}

    @pytest.mark.asyncio
    async def test_an_outage_is_the_named_result(self, live, tmp_path, monkeypatch):
        from core.chat_tools import DATA_UNAVAILABLE, execute_tool
        from tests.helpers.off_consumers import AssistantTools
        from tests.helpers.off_consumers import _duck

        pool, _get_pool = live
        store = await _duck(tmp_path)
        AssistantTools._serve_tools(monkeypatch, store)
        await pool.close()

        result = await execute_tool("list_expenses", {})

        assert result["error"] == DATA_UNAVAILABLE
        assert result["surface"] == "expenses"
