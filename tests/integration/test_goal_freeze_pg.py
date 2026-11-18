"""Revision 0037 and the month goal freeze against a real PostgreSQL.

The suite's database is at the head. The revision's statements and Alembic's
own rendering of the downgrade and the upgrade run inside a transaction that
is always rolled back. What the freeze writes is cleared before and after by
`tab_in_postgres`, which counts `app.frozen_month_goals` among the tables it
clears, so no month frozen here answers another test's smart goal.

Pinned, each named by the mutation it kills:

- the table as the catalogue reports it — the key, the month CHECK, the NOT
  NULLs — read by `ks_readonly` and writable by it nowhere;
- 0037 applies twice; the downgrade moves the stamp and keeps every row, and
  the upgrade after it is a no-op;
- the freeze writes once and only once a month: a second run keeps the first
  answer and its stamp, and two runs at once leave one row per sales type,
  all six from the same run;
- the frozen answer is served though every live input moves — Silver's
  revenue, the stored forecast — while a live computation moves with them;
- a typed goal overrides a frozen one; a month with no row is computed live
  and stays unfrozen; the two GETs, through the app, write nothing;
- the job, end to end.
"""
from __future__ import annotations

import asyncio
import calendar
import json
import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import asyncpg
import pytest
import pytest_asyncio

from core import goal_freeze, pg
from tests import migration_replay as replay
from tests.integration.pg_tab_seed import store_that_never_reaches_duckdb, tab_in_postgres
from tests.unit.test_goals_off_duckdb_silver import _seed_history_pg
from tests.unit.test_goals_read_paths_do_not_write import _ConnectPerAcquire
from tests.unit.test_read_fallback_sites import (  # noqa: F401 — fixtures
    admin_client,
    fresh_read_fallback,
)

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

REPO = Path(__file__).resolve().parents[2]
REV = replay.load(replay.VERSIONS / "0037_frozen_month_goals.py")
BEFORE = "0036_analytics_captures"
REV_ID = "0037_frozen_month_goals"
# The database's head: 0037 until a later revision descends from it.
CURRENT = pg.REQUIRED_REVISION
TABLE = "app.frozen_month_goals"
KYIV = ZoneInfo("Europe/Kyiv")

# A month the fixture's history answers for: October 2025 is the year before.
OCTOBER = datetime(2026, 10, 5, 12, 0, tzinfo=KYIV)


def _alembic_sql(*args: str) -> str:
    """What `alembic <args> --sql` would execute, without its BEGIN/COMMIT, so
    it can run inside the test's own transaction."""
    out = subprocess.run(
        [sys.executable, "-m", "alembic", *args, "--sql"],
        cwd=REPO, capture_output=True, text=True, timeout=120,
        env={"PATH": "/usr/bin:/bin", "HOME": "/tmp"},
    )
    assert out.returncode == 0, out.stderr[-2000:]
    return "\n".join(line for line in out.stdout.splitlines()
                     if line.strip() not in ("BEGIN;", "COMMIT;"))


async def _stamp(conn) -> str:
    return await conn.fetchval("SELECT version_num FROM meta.alembic_version")


def _one_row(month: int = 1) -> str:
    """A row in a month no freeze here touches (January 2000)."""
    return ("INSERT INTO app.frozen_month_goals (year, month, sales_type, smart, frozen_at) "
            f"VALUES (2000, {month}, 'retail', '{{\"monthly\": {{\"goal\": 1}}}}'::jsonb, now())")


class TestTheRevisionOnAServer:
    @pytest.mark.asyncio
    async def test_the_table_is_there_as_the_catalogue_reads_it(self):
        """Kills: a key without `sales_type` (one goal for every sales type),
        the month CHECK lost, a nullable answer or stamp."""
        conn = await asyncpg.connect(DSN)
        try:
            assert await _stamp(conn) == CURRENT
            assert REV_ID in [m.revision for m in replay.lineage(CURRENT)]
            columns = {r["column_name"]: (r["data_type"], r["is_nullable"]) for r in await conn.fetch(
                "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
                "WHERE table_schema = 'app' AND table_name = 'frozen_month_goals'")}
            assert columns == {
                "year": ("smallint", "NO"), "month": ("smallint", "NO"),
                "sales_type": ("character varying", "NO"), "smart": ("jsonb", "NO"),
                "frozen_at": ("timestamp with time zone", "NO")}
            key = await conn.fetchval(
                "SELECT array_agg(a.attname ORDER BY k.ord) FROM pg_index i "
                "CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) "
                "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum "
                "WHERE i.indrelid = $1::regclass AND i.indisprimary", TABLE)
            assert key == ["year", "month", "sales_type"]
            tx = conn.transaction()
            await tx.start()
            try:
                with pytest.raises(asyncpg.CheckViolationError):
                    await conn.execute(_one_row(13))
            finally:
                await tx.rollback()
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_ks_readonly_reads_it_and_writes_nothing(self):
        """The default privileges in `postgres/initdb/30-app.sql`, not a GRANT
        in the revision."""
        conn = await asyncpg.connect(DSN)
        try:
            assert await conn.fetchval(
                "SELECT has_table_privilege('ks_readonly', $1, 'SELECT')", TABLE)
            for right in ("INSERT", "UPDATE", "DELETE", "TRUNCATE"):
                assert not await conn.fetchval(
                    "SELECT has_table_privilege('ks_readonly', $1, $2)", TABLE, right), right
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_it_applies_twice(self):
        """Every statement again on a database that has them, then Alembic's
        own rendering of the step from 0036. Kills: an IF NOT EXISTS dropped,
        or a COMMENT that does not parse on a server."""
        conn = await asyncpg.connect(DSN)
        tx = conn.transaction()
        await tx.start()
        try:
            for statement in REV.STATEMENTS:
                await conn.execute(statement)
            await conn.execute("UPDATE meta.alembic_version SET version_num = $1", BEFORE)
            await conn.execute(_alembic_sql("upgrade", f"{BEFORE}:{CURRENT}"))
            assert await _stamp(conn) == CURRENT
        finally:
            await tx.rollback()
            await conn.close()

    @pytest.mark.asyncio
    async def test_the_downgrade_moves_the_stamp_and_keeps_the_rows(self):
        """As Alembic renders it and the server runs it: down to 0036 the stamp
        is 0036, an image at 0036's check passes and the frozen goal is still
        there; up again, the stamp is the head and nothing else moved. Kills:
        a DROP in the downgrade, or a re-upgrade that is not a no-op."""
        conn = await asyncpg.connect(DSN)
        tx = conn.transaction()
        await tx.start()
        try:
            await conn.execute(_one_row())
            before = await conn.fetch(f"SELECT * FROM {TABLE} ORDER BY sales_type")
            await conn.execute(_alembic_sql("downgrade", f"{CURRENT}:{BEFORE}"))
            assert await _stamp(conn) == BEFORE
            await pg.require_revision(BEFORE, conn=conn)
            assert await conn.fetch(f"SELECT * FROM {TABLE} ORDER BY sales_type") == before

            await conn.execute(_alembic_sql("upgrade", f"{BEFORE}:{CURRENT}"))
            assert await _stamp(conn) == CURRENT
            assert await conn.fetch(f"SELECT * FROM {TABLE} ORDER BY sales_type") == before
        finally:
            await tx.rollback()
            await conn.close()


# ─── The freeze ─────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def history(tmp_path, monkeypatch):
    """The three-year history in Postgres' Bronze, Silver and Gold derived
    from it, the frozen goals empty, and a store that never reaches DuckDB:
    `(store, pool)`."""
    async with tab_in_postgres(DSN, monkeypatch, _seed_history_pg) as pool:
        async with pool.acquire() as conn:
            assert await conn.fetchval(f"SELECT COUNT(*) FROM {TABLE}") == 0
        yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the goal freeze"), pool


async def _rows(pool, year=None, month=None) -> dict:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT year, month, sales_type, smart::text AS smart, frozen_at FROM {TABLE} "
            "WHERE ($1::int IS NULL OR year = $1) AND ($2::int IS NULL OR month = $2)",
            year, month)
    return {(r["year"], r["month"], r["sales_type"]): (r["smart"], r["frozen_at"]) for r in rows}


async def _scale_silver(pool, factor: float) -> None:
    async with pool.acquire() as conn:
        await conn.execute("UPDATE silver.orders SET grand_total = grand_total * $1", factor)


def _this_month_at() -> datetime:
    """Now, in Kyiv — moved past 00:30 if this is a 1st before it, so the
    freeze under test never turns itself away whatever day the suite runs."""
    now = datetime.now(KYIV)
    return now.replace(hour=0, minute=31) if goal_freeze.too_early(now) else now


async def _predict_rest_of_month(pool, value: float) -> None:
    """A retail forecast of `value` a day from today to the month's last day."""
    today = datetime.now(KYIV).date()
    last = date(today.year, today.month, calendar.monthrange(today.year, today.month)[1])
    days = [today + timedelta(days=n) for n in range((last - today).days + 1)]
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM app.revenue_predictions WHERE sales_type = 'retail'")
        await conn.executemany(
            "INSERT INTO app.revenue_predictions (prediction_date, sales_type, "
            "predicted_revenue, model_mae, model_mape, model_wape, created_at) "
            "VALUES ($1, 'retail', $2, 1, 1, 1, now())", [(d, value) for d in days])


class TestOnceAMonth:
    @pytest.mark.asyncio
    async def test_the_first_run_freezes_every_sales_type_and_a_second_changes_nothing(
        self, history,
    ):
        """Kills: an upsert where the first freeze must win — a second run, a
        manual trigger, would move a frozen goal; or a run that recomputes."""
        store, pool = history
        first = await goal_freeze.freeze_month(store, now=OCTOBER)
        assert first == {"month": "2026-10", "frozen": list(goal_freeze.SALES_TYPES),
                         "already": []}
        kept = await _rows(pool)
        assert set(kept) == {(2026, 10, s) for s in goal_freeze.SALES_TYPES}
        assert {stamp for _, stamp in kept.values()} == {OCTOBER}
        # What is kept is the answer, whole: the arithmetic's own numbers.
        retail = json.loads(kept[(2026, 10, "retail")][0])
        assert retail["targetMonth"] == 10 and retail["monthly"]["lastYearRevenue"] > 0
        live = await store.generate_smart_goals(2026, 10, "retail")
        assert retail["monthly"]["lastYearRevenue"] == live["monthly"]["lastYearRevenue"]

        await _scale_silver(pool, 1.5)
        second = await goal_freeze.freeze_month(store, now=OCTOBER + timedelta(days=3))
        assert second == {"month": "2026-10", "frozen": [],
                          "already": sorted(goal_freeze.SALES_TYPES)}
        assert await _rows(pool) == kept

    @pytest.mark.asyncio
    async def test_two_runs_at_once_leave_one_row_each_all_from_one_run(
        self, history, monkeypatch,
    ):
        """Both runs past the read of what the month holds before either
        writes, so both compute and both insert: `ON CONFLICT DO NOTHING` on
        the real key lets one run's six rows in and the other's none. Kills:
        a plain INSERT (the second run fails, or with an upsert overwrites),
        or rows written outside one transaction (a month mixed from two
        runs)."""
        store, pool = history
        real_held = goal_freeze._held
        arrived, both = [], asyncio.Event()

        async def held_together(pool_, year, month):
            held = await real_held(pool_, year, month)
            arrived.append(1)
            if len(arrived) == 2:
                both.set()
            await asyncio.wait_for(both.wait(), 30)
            return held

        monkeypatch.setattr(goal_freeze, "_held", held_together)
        later = OCTOBER + timedelta(seconds=1)
        a, b = await asyncio.gather(goal_freeze.freeze_month(store, now=OCTOBER),
                                    goal_freeze.freeze_month(store, now=later))

        assert sorted(a["frozen"] + b["frozen"]) == sorted(goal_freeze.SALES_TYPES)
        assert {len(a["frozen"]), len(b["frozen"])} == {0, len(goal_freeze.SALES_TYPES)}
        kept = await _rows(pool)
        assert set(kept) == {(2026, 10, s) for s in goal_freeze.SALES_TYPES}
        assert len({stamp for _, stamp in kept.values()}) == 1


class TestTheFrozenAnswerIsServed:
    @pytest.mark.asyncio
    async def test_though_silver_moves_under_it(self, history):
        """The past-month case: frozen, then every order's revenue moves. The
        GET answers the frozen goal; a live computation moves with the data.
        Kills: the reader computing live whatever the table holds."""
        store, pool = history
        await goal_freeze.freeze_month(store, now=OCTOBER)
        frozen = await store.get_smart_goals("retail", year=2026, month=10)

        await _scale_silver(pool, 1.5)
        live = await store.generate_smart_goals(2026, 10, "retail")
        served = await store.get_smart_goals("retail", year=2026, month=10)

        assert live["monthly"]["lastYearRevenue"] == pytest.approx(
            1.5 * frozen["monthly"]["lastYearRevenue"], rel=1e-6)
        assert served == frozen
        assert served["monthly"]["frozenAt"] == OCTOBER.isoformat()
        forecast = await store.get_month_goal(2026, 10, "retail")
        assert forecast["monthly"]["lastYearRevenue"] == frozen["monthly"]["lastYearRevenue"]
        assert forecast["monthly"]["frozenAt"] == OCTOBER.isoformat()

    @pytest.mark.asyncio
    async def test_though_the_forecast_moves_under_it(self, history):
        """The month in progress, whose ML term is the forecast of the rest of
        it: a training that doubles the forecast moves the live answer and
        not the frozen one — the drift this revision exists to stop. Kills:
        the ML signal read live beside a frozen answer."""
        store, pool = history
        await _predict_rest_of_month(pool, 10_000.0)
        at = _this_month_at()
        result = await goal_freeze.freeze_month(store, now=at)
        assert result["frozen"] == list(goal_freeze.SALES_TYPES)
        frozen = await store.get_smart_goals("retail")
        assert frozen["monthly"]["mlForecastGoal"] > 0

        await _predict_rest_of_month(pool, 20_000.0)
        live = await store.generate_smart_goals(at.year, at.month, "retail")
        served = await store.get_smart_goals("retail")

        assert live["monthly"]["mlForecastGoal"] > frozen["monthly"]["mlForecastGoal"]
        assert served["monthly"]["mlForecastGoal"] == frozen["monthly"]["mlForecastGoal"]
        assert served["monthly"]["suggestedAmount"] == frozen["monthly"]["suggestedAmount"]
        assert served["monthly"]["frozenAt"] is not None

    @pytest.mark.asyncio
    async def test_a_typed_goal_still_overrides_it(self, history):
        store, pool = history
        await goal_freeze.freeze_month(store, now=OCTOBER)
        automatic = (await store.get_smart_goals("retail", year=2026, month=10))["monthly"]
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO app.revenue_goals (period_type, goal_amount, is_custom, "
                "calculated_goal, growth_factor, updated_at) "
                "VALUES ('monthly', 7777700, TRUE, NULL, 1.1, now())")
        monthly = (await store.get_smart_goals("retail", year=2026, month=10))["monthly"]

        assert monthly["amount"] == 7_777_700
        assert monthly["isCustom"] is True
        assert monthly["suggestedAmount"] == automatic["suggestedAmount"]
        assert monthly["frozenAt"] == OCTOBER.isoformat()

    @pytest.mark.asyncio
    async def test_a_month_with_no_row_is_computed_live_and_stays_unfrozen(self, history):
        """Every month before the deploy. Kills: a GET that freezes what it
        reads, or a missing row read as no goal."""
        store, pool = history
        await goal_freeze.freeze_month(store, now=OCTOBER)
        september = await store.get_smart_goals("b2b", year=2026, month=9)
        live = await store.generate_smart_goals(2026, 9, "b2b")

        assert september["monthly"]["frozenAt"] is None
        assert september["monthly"]["suggestedAmount"] == live["monthly"]["goal"]
        assert september["monthly"]["lastYearRevenue"] == live["monthly"]["lastYearRevenue"]
        assert not await _rows(pool, 2026, 9)


class TestAShortForecastWaits:
    @pytest.mark.asyncio
    async def test_on_the_1st_it_waits_for_a_forecast_that_reaches_the_months_end(
        self, history, monkeypatch,
    ):
        """1 December 2026, 00:30, and the stored retail forecast holds 28 of
        December's 31 days, as after a month without a training: the ML
        signal summed 28 days, and kept, the goal would stand too low all
        month. Retail is read live and named under `waiting`, the others
        freeze; the run after the missing days are stored freezes retail
        with the whole month's forecast in its signal. The signal's clock is
        the freeze's, the 1st's. Kills: the freeze keeping a short answer, or
        holding back a sales type whose answer carries no signal."""
        store, pool = history
        at = datetime(2026, 12, 1, 0, 30, tzinfo=KYIV)

        class _OnThe1st(datetime):
            @classmethod
            def now(cls, tz=None):
                return at.astimezone(tz) if tz else at.replace(tzinfo=None)

        monkeypatch.setattr("core.repositories.goals.datetime", _OnThe1st)
        december = [date(2026, 12, d) for d in range(1, 32)]
        insert = ("INSERT INTO app.revenue_predictions (prediction_date, sales_type, "
                  "predicted_revenue, model_mae, model_mape, model_wape, created_at) "
                  "VALUES ($1, 'retail', $2, 1, 1, 1, now())")
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM app.revenue_predictions WHERE sales_type = 'retail'")
            await conn.executemany(insert, [(d, 10_000.0) for d in december[:28]])

        first = await goal_freeze.freeze_month(store, now=at)
        assert first["waiting"] == {
            "retail": "the stored forecast holds 28 of the 31 days to the month's end"}
        assert first["frozen"] == [s for s in goal_freeze.SALES_TYPES if s != "retail"]
        assert (2026, 12, "retail") not in await _rows(pool, 2026, 12)
        live = await store.get_smart_goals("retail", year=2026, month=12)
        assert live["monthly"]["frozenAt"] is None
        assert live["monthly"]["mlForecastGoal"] == pytest.approx(28 * 10_000.0)

        async with pool.acquire() as conn:
            await conn.executemany(insert, [(d, 10_000.0) for d in december[28:]])
        second = await goal_freeze.freeze_month(store, now=at + timedelta(hours=6))
        assert second["frozen"] == ["retail"] and "waiting" not in second
        kept = json.loads((await _rows(pool, 2026, 12))[(2026, 12, "retail")][0])
        assert kept["monthly"]["mlForecastGoal"] == pytest.approx(31 * 10_000.0)


class TestNoGetWrites:
    @pytest.mark.asyncio
    async def test_the_two_gets_through_the_app_write_nothing(
        self, history, admin_client, fresh_read_fallback,  # noqa: F811
    ):
        """`/api/goals/smart` and `/api/goals/forecast` for a month with no
        row, and for one that is frozen: the table afterwards is what it was,
        and the frozen month is answered frozen. Kills: a GET that froze the
        month it was asked about."""
        store, pool = history
        with patch("core.pg.get_pool", new=AsyncMock(return_value=_ConnectPerAcquire(DSN))), \
                patch("core.pg.require_revision", new=AsyncMock()):
            for path in ("/api/goals/smart", "/api/goals/forecast"):
                response = admin_client.get(path, params={"year": 2026, "month": 10})
                assert response.status_code == 200, response.text
            assert await _rows(pool) == {}

        await goal_freeze.freeze_month(store, now=OCTOBER)
        kept = await _rows(pool)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=_ConnectPerAcquire(DSN))), \
                patch("core.pg.require_revision", new=AsyncMock()):
            smart = admin_client.get("/api/goals/smart", params={"year": 2026, "month": 10})
            forecast = admin_client.get("/api/goals/forecast", params={"year": 2026, "month": 10})
        assert smart.json()["monthly"]["frozenAt"] == OCTOBER.isoformat()
        assert forecast.json()["monthly"]["frozenAt"] == OCTOBER.isoformat()
        retail = json.loads(kept[(2026, 10, "retail")][0])
        assert forecast.json()["monthly"]["goal"] == retail["monthly"]["goal"]
        assert forecast.json()["weekly"]["breakdown"] == {
            str(k): v for k, v in retail["weekly"]["breakdown"].items()}
        assert await _rows(pool) == kept


class TestTheJob:
    @pytest.mark.asyncio
    async def test_end_to_end_it_freezes_this_month_once(self, history, monkeypatch):
        from core.scheduler import BackgroundScheduler

        store, pool = history
        monkeypatch.setattr("core.store.get_store", AsyncMock(return_value=store))
        monkeypatch.setattr(goal_freeze, "too_early", lambda at: False)
        now = datetime.now(KYIV)

        first = await BackgroundScheduler()._run_month_goal_freeze()
        second = await BackgroundScheduler()._run_month_goal_freeze()

        assert first["frozen"] == list(goal_freeze.SALES_TYPES)
        assert second["frozen"] == [] and second["already"] == sorted(goal_freeze.SALES_TYPES)
        assert set(await _rows(pool)) == {(now.year, now.month, s) for s in goal_freeze.SALES_TYPES}
