"""`/goals` means the same thing in DuckDB and Postgres.

`tests/unit/test_pg_goals_read.py` proves the bodies are one text, that nothing
writes through the router, and that the window is not bound with
`CURRENT_DATE`. All of that is about the SQL. This is about what the two
engines *do* with it, and there are four places they could part company:

  * **the parameters.** `get_daily_revenue_for_dates` hands its dates over as
    ISO strings, which DuckDB coerces to `DATE` without being asked. asyncpg
    does not coerce — it encodes by the type Postgres infers — so a string
    against a `date` column is the kind of thing that raises at the driver and
    is invisible to a unit test that mocks `fetch`.
  * **`DATE_TRUNC('week', …)`** — both engines have it, and a week that began
    on a different day would move every weekly figure by up to six days'
    revenue.
  * **`STDDEV`** — sample or population, and what it returns over one row.
  * **`INTERVAL 'n days'` and `'n months'`** against a date.

The fixture is small on purpose: these read one column of one table, so the
awkwardness that matters is in the shapes above, not in the catalogue. It does
carry a return, a retired source and a b2b order, because the predicates the
port replaced were exactly the ones that decide those three.

Skipped without `KS_PG_DSN`; `deploy/gate_with_stores.sh` supplies one.
"""
from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch

from core.duckdb_constants import B2B_MANAGER_ID
from core.duckdb_store import DuckDBStore

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

# The fixture has to think in the warehouse's timezone, not the runner's.
#
# `order_date` is computed by `_date_in_kyiv()`, so a row seeded at
# `datetime.now(timezone.utc) - 3 days` is dated by its *Kyiv* date. `TODAY`
# was `date.today()`, which is the date where the test happens to be running —
# UTC under `deploy/quick_gate.sh` and in CI. Those two agree for twenty-one
# hours a day and disagree from 21:00 UTC, when Kyiv has already turned over:
# the query then asked for 2026-09-10 while the fixture had written
# 2026-09-11, `get_daily_revenue_for_dates` answered `{}` from both engines,
# and the assertion that caught it was the one checking the result was not
# empty. Seen on 2026-09-13, sixteen minutes after midnight in Kyiv, against
# a CI run that had been green at 20:49 UTC on the same commit.
#
# Fixed at the root rather than by shifting the comparison: the seed is
# anchored to **midday in Kyiv**, so `anchor - n days` is midday on
# `TODAY - n` in every hour of the day and on both sides of a DST change.
# Nothing here now depends on when the suite runs.
#
# Nine sibling `*_two_engines.py` files open with the same `date.today()` and
# are deliberately not touched: none of them fails today, and rewriting ten
# fixtures on the strength of one proof is how nine working tests break.
KYIV = ZoneInfo("Europe/Kyiv")
TODAY = datetime.now(timezone.utc).astimezone(KYIV).date()

# Eight weeks of history so the weekly grain has something to trend over, and
# the monthly grain more than one bucket.
# (order_id, source_id, grand_total, days_ago, is_return, manager_id)
ORDERS = [
    (1, 1, 1200.0, 3, False, None),
    (2, 2, 600.0, 5, False, None),
    (3, 1, 400.0, 9, True, None),        # a return — excluded by `is_return`
    (4, 3, 900.0, 12, False, None),      # source 3 retired — `is_active_source`
    (5, 4, 750.0, 17, False, None),
    (6, 1, 300.0, 23, False, B2B_MANAGER_ID),   # b2b
    (7, 2, 1500.0, 31, False, None),
    (8, 1, 800.0, 45, False, None),
    (9, 4, 250.0, 52, False, None),
]

PG_TABLES = ("gold.daily_revenue", "silver.orders", "app.revenue_goals",
             "app.revenue_predictions")

GOALS = [
    ("daily", 5000.0, True, 4500.0, 1.10),
    ("weekly", 30000.0, False, 31000.0, 1.05),
]


async def _seed_duckdb(store):
    # Midday in Kyiv on `TODAY`, so every `now - n days` lands at midday on
    # `TODAY - n` — twelve hours from either midnight, which is what makes the
    # date arithmetic immune both to the hour the suite runs and to the one-hour
    # DST step. See the note on `TODAY`.
    now = datetime.combine(TODAY, time(12, 0), tzinfo=KYIV)
    async with store.connection() as conn:
        for oid, src, total, days, is_ret, manager in ORDERS:
            conn.execute(
                "INSERT INTO orders (id, source_id, status_id, grand_total,"
                " ordered_at, buyer_id, manager_id) VALUES (?,?,?,?,?,7,?)",
                [oid, src, 19 if is_ret else 1, total,
                 now - timedelta(days=days), manager])
        conn.executemany(
            "INSERT INTO revenue_goals (period_type, goal_amount, is_custom,"
            " calculated_goal, growth_factor) VALUES (?,?,?,?,?)", GOALS)
    await store.refresh_warehouse_layers(trigger="manual")


async def _seed_postgres(conn, store):
    for table in PG_TABLES:
        await conn.execute(f"DELETE FROM {table}")

    async with store.connection() as duck:
        silver = duck.execute(
            "SELECT id, source_id, status_id, grand_total, ordered_at, buyer_id,"
            " manager_id, order_date, is_return, sales_type, is_active_source,"
            " source_name, is_new_customer, buyer_first_order_date, promocode"
            " FROM silver_orders ORDER BY id").fetchall()

    await conn.executemany(
        "INSERT INTO silver.orders (id,source_id,status_id,grand_total,ordered_at,"
        "buyer_id,manager_id,order_date,is_return,sales_type,is_active_source,"
        "source_name,is_new_customer,buyer_first_order_date,promocode)"
        " VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
        [tuple(r) for r in silver])
    await conn.executemany(
        "INSERT INTO app.revenue_goals (period_type,goal_amount,is_custom,"
        "calculated_goal,growth_factor,updated_at) VALUES ($1,$2,$3,$4,$5,now())",
        GOALS)


@pytest_asyncio.fixture
async def both_engines(tmp_path, monkeypatch):
    monkeypatch.setenv("KS_PG_DSN", DSN)
    store = DuckDBStore(db_path=tmp_path / "goals.duckdb")
    await store.connect()
    pool = await asyncpg.create_pool(DSN, min_size=1, max_size=3)
    try:
        await _seed_duckdb(store)
        async with pool.acquire() as conn:
            await _seed_postgres(conn, store)
        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            yield store
    finally:
        async with pool.acquire() as conn:
            for table in PG_TABLES:
                await conn.execute(f"DELETE FROM {table}")
        await pool.close()
        await store.close()


async def _both(store, monkeypatch, name, args=(), kwargs=None):
    """Each engine once — and the Postgres leg with DuckDB made fatal."""
    kwargs = kwargs or {}
    monkeypatch.delenv("KS_READ_GOALS", raising=False)
    duck = await getattr(store, name)(*args, **kwargs)

    monkeypatch.setenv("KS_READ_GOALS", "postgres")

    def _no_duckdb(*_a, **_k):
        raise AssertionError(
            f"{name} fell back to DuckDB — Postgres did not answer, so the "
            f"comparison would have compared DuckDB with itself"
        )

    with patch.object(type(store), "connection", _no_duckdb):
        postgres = await getattr(store, name)(*args, **kwargs)
    monkeypatch.delenv("KS_READ_GOALS", raising=False)
    return duck, postgres


def _comparable(value):
    """Money to six places: DuckDB divides a DECIMAL into a DOUBLE and Postgres
    into a NUMERIC, so the two legitimately differ in the last bits."""
    if isinstance(value, bool):
        return value
    if isinstance(value, dict):
        return {k: _comparable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_comparable(v) for v in value]
    if hasattr(value, "as_tuple") or isinstance(value, float):
        return round(float(value), 6)
    return value


class TestTheHistoryAgrees:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("period", ["daily", "weekly", "monthly"])
    @pytest.mark.parametrize("sales_type", ["retail", "all", "b2b"])
    async def test_every_grain_and_every_sales_type(
        self, both_engines, monkeypatch, period, sales_type,
    ):
        duck, postgres = await _both(
            both_engines, monkeypatch, "get_historical_revenue",
            (period,), {"weeks_back": 8, "sales_type": sales_type})
        assert _comparable(duck) == _comparable(postgres)

    @pytest.mark.asyncio
    async def test_the_weekly_trend_branch_runs_on_both(
        self, both_engines, monkeypatch,
    ):
        """`trend` is a second statement, reached only when the weekly grain
        found four buckets or more — so it needs its own case, or the branch
        never executes under comparison."""
        duck, postgres = await _both(
            both_engines, monkeypatch, "get_historical_revenue",
            ("weekly",), {"weeks_back": 8})
        assert _comparable(duck) == _comparable(postgres)
        assert duck["periodCount"] >= 4, (
            "the fixture no longer reaches the trend branch — widen it, or "
            "this test silently stops covering the second statement"
        )


class TestTheDatesGoOverAsDates:
    """The one a mocked `fetch` cannot see: DuckDB coerces an ISO string to a
    DATE, and asyncpg encodes by the type Postgres infers."""

    @pytest.mark.asyncio
    async def test_a_list_of_dates_answers_from_postgres(
        self, both_engines, monkeypatch,
    ):
        dates = [TODAY - timedelta(days=n) for n in (3, 5, 9, 12)]
        duck, postgres = await _both(
            both_engines, monkeypatch, "get_daily_revenue_for_dates", (dates,))
        assert _comparable(duck) == _comparable(postgres)
        assert postgres, "no rows came back — the window missed the fixture"

    @pytest.mark.asyncio
    async def test_the_keys_are_dates_on_both(self, both_engines, monkeypatch):
        """The caller looks the result up by `date`. A string key would miss
        every lookup and show an empty comparison line rather than an error."""
        dates = [TODAY - timedelta(days=3)]
        duck, postgres = await _both(
            both_engines, monkeypatch, "get_daily_revenue_for_dates", (dates,))
        for result in (duck, postgres):
            for key in result:
                assert isinstance(key, date), (key, type(key))

    @pytest.mark.asyncio
    async def test_an_empty_list_asks_neither_engine(
        self, both_engines, monkeypatch,
    ):
        duck, postgres = await _both(
            both_engines, monkeypatch, "get_daily_revenue_for_dates", ([],))
        assert duck == postgres == {}


class TestTheStoredGoalsAgree:
    @pytest.mark.asyncio
    async def test_get_goals_reads_the_same_rows(self, both_engines, monkeypatch):
        duck, postgres = await _both(both_engines, monkeypatch, "get_goals")
        assert _comparable(duck) == _comparable(postgres)

    @pytest.mark.asyncio
    async def test_is_custom_survives_both_encodings(
        self, both_engines, monkeypatch,
    ):
        """`is_custom` is BOOLEAN in Postgres and comes back an integer from
        DuckDB. `get_smart_goals` narrows on it in Python for exactly that
        reason, and the daily goal above is the custom one."""
        duck, postgres = await _both(both_engines, monkeypatch, "get_goals")
        assert duck["daily"]["isCustom"] == postgres["daily"]["isCustom"]
        assert bool(postgres["daily"]["isCustom"]) is True
        assert bool(postgres["weekly"]["isCustom"]) is False


PREDICTIONS = [
    # (prediction_date, sales_type, predicted_revenue, mae, mape, wape)
    (TODAY - timedelta(days=2), "retail", 101_000.00, 900.0, 5.5, 6.1),
    (TODAY - timedelta(days=1), "retail", 102_500.50, 900.0, 5.5, 6.1),
    (TODAY, "retail", 99_999.99, 900.0, 5.5, 6.1),
    (TODAY, "b2b", 12_000.00, 300.0, 9.9, 8.8),   # another sales type, same day
]


class TestTheForecastReadsTheSameOnBoth:
    """`get_predictions` is the last read of this tab to move, and it could not
    until revision 0025 gave the table a Postgres home.

    The case that matters is not the numbers — five columns copied verbatim —
    but the **parameters**: the method used to hand its bounds over as ISO
    strings, which DuckDB coerces to DATE and asyncpg refuses. That is invisible
    to a unit test, which mocks `fetch` and never meets the driver, and in
    production it does not break the page: the read falls back, logs an ERROR
    and answers from DuckDB while the tab looks switched.
    """

    @pytest_asyncio.fixture
    async def seeded(self, both_engines):
        store = both_engines
        async with store.connection() as conn:
            conn.executemany(
                "INSERT INTO revenue_predictions (prediction_date, sales_type,"
                " predicted_revenue, model_mae, model_mape, model_wape)"
                " VALUES (?,?,?,?,?,?)", PREDICTIONS)
        from core.pg import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.executemany(
                "INSERT INTO app.revenue_predictions (prediction_date, sales_type,"
                " predicted_revenue, model_mae, model_mape, model_wape)"
                " VALUES ($1,$2,$3,$4,$5,$6)", PREDICTIONS)
        return store

    @pytest.mark.asyncio
    async def test_a_window_returns_the_same_rows(self, seeded, monkeypatch):
        duck, postgres = await _both(
            seeded, monkeypatch, "get_predictions",
            (TODAY - timedelta(days=3), TODAY))
        assert _comparable(duck) == _comparable(postgres)
        assert len(postgres) == 3, "retail only, and all three days of it"

    @pytest.mark.asyncio
    async def test_the_sales_type_filter_separates_the_two_rows_of_one_day(
        self, seeded, monkeypatch,
    ):
        duck, postgres = await _both(
            seeded, monkeypatch, "get_predictions", (TODAY, TODAY),
            {"sales_type": "b2b"})
        assert _comparable(duck) == _comparable(postgres)
        assert len(postgres) == 1 and postgres[0]["predicted_revenue"] == 12_000.00

    @pytest.mark.asyncio
    async def test_an_empty_window_is_empty_on_both(self, seeded, monkeypatch):
        duck, postgres = await _both(
            seeded, monkeypatch, "get_predictions",
            (TODAY - timedelta(days=400), TODAY - timedelta(days=390)))
        assert duck == postgres == []
