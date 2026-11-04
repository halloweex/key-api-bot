"""`/goals` answers in Postgres what DuckDB answered.

`tests/unit/test_pg_goals_read.py` proves the bodies are one text, that nothing
writes through the router, and that the window is not bound with
`CURRENT_DATE`. All of that is about the SQL. This is about what the engine
*does* with it, and there were four places the two engines could part company:

  * **the parameters.** `get_daily_revenue_for_dates` hands its dates over as
    `date`s: DuckDB coerced an ISO string to `DATE` without being asked, and
    asyncpg does not — it encodes by the type Postgres infers — so a string
    against a `date` column is the kind of thing that raises at the driver and
    is invisible to a unit test that mocks `fetch`.
  * **`DATE_TRUNC('week', …)`** — a week that began on a different day would
    move every weekly figure by up to six days' revenue.
  * **`STDDEV`** — sample or population, and what it returns over one row.
  * **`INTERVAL 'n days'` and `'n months'`** against a date.

ONE ENGINE SINCE STEP 14 (S14-07)

Every goal read is Postgres'. Each call below is compared with a literal
(`tests/integration/golden/goals.json`): DuckDB's answer on this fixture,
recorded while the same run asserted Postgres answered each one equally (rule
9 of the step-14 plan). The fixture and every clock the reads ask are pinned
to `TODAY` (`_frozen_clock`), because the weekly and monthly grains bucket by
the calendar. Postgres derives Silver and Gold from the seeded Bronze — the
production path — and the store's DuckDB connection is a trap.

The fixture is small on purpose: these read one column of one table, so the
awkwardness that matters is in the shapes above, not in the catalogue. It does
carry a return, a retired source and a b2b order, because the predicates the
port replaced were exactly the ones that decide those three.

Skipped without `KS_PG_DSN`; CI and `deploy/gate_with_stores.sh` supply one.
"""
from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

from core.duckdb_constants import B2B_MANAGER_ID
from tests.integration.pg_tab_seed import (
    call_key, comparable, derive, golden, seed, store_that_never_reaches_duckdb,
    tab_in_postgres,
)

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(
    not DSN, reason="needs a live PostgreSQL at KS_PG_DSN",
)

# The fixture thinks in the warehouse's timezone, not the runner's: the seed
# is anchored to **midday in Kyiv**, so `anchor - n days` is midday on
# `TODAY - n` in every hour of the day and on both sides of a DST change
# (seen failing on 2026-09-13, sixteen minutes after midnight in Kyiv, when it
# read the runner's day).
KYIV = ZoneInfo("Europe/Kyiv")
# Pinned (step 14, S14-07), and the clock with it (`_frozen_clock`): the
# answers are literals taken from DuckDB on this fixture, and the weekly and
# monthly grains bucket by the calendar, so the day the suite runs would
# otherwise move them.
TODAY = date(2026, 9, 15)

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

GOALS = [
    ("daily", 5000.0, True, 4500.0, 1.10),
    ("weekly", 30000.0, False, 31000.0, 1.05),
]

PREDICTIONS = [
    # (prediction_date, sales_type, predicted_revenue, mae, mape, wape)
    (TODAY - timedelta(days=2), "retail", 101_000.00, 900.0, 5.5, 6.1),
    (TODAY - timedelta(days=1), "retail", 102_500.50, 900.0, 5.5, 6.1),
    (TODAY, "retail", 99_999.99, 900.0, 5.5, 6.1),
    (TODAY, "b2b", 12_000.00, 300.0, 9.9, 8.8),   # another sales type, same day
]


class _FrozenClock(datetime):
    """`datetime` with `now()` at midday in Kyiv on `TODAY`."""

    @classmethod
    def now(cls, tz=None):
        moment = datetime.combine(TODAY, time(12, 0), tzinfo=KYIV)
        return moment.astimezone(tz) if tz else moment.replace(tzinfo=None)


@pytest.fixture(autouse=True)
def _frozen_clock(monkeypatch):
    """Every clock these reads ask, at `TODAY`: the goal repository's Python
    clock, the forecast service's Kyiv day, and the SQL's own "today" — the
    one body that reads it (`get_historical_revenue`) imports
    `TODAY_IN_KYIV` at the call, so the pinned date reaches the engine.
    `DATE '…'` reads the same in DuckDB, in which the literals were recorded,
    and Postgres."""
    monkeypatch.setattr("core.repositories.goals.datetime", _FrozenClock)
    monkeypatch.setattr("core.prediction_service._today_kyiv", lambda: TODAY)
    monkeypatch.setattr("core.sql_dialect.TODAY_IN_KYIV", f"DATE '{TODAY.isoformat()}'")


def _populate_with(predictions):
    async def populate(conn, pool):
        anchor = datetime.combine(TODAY, time(12, 0), tzinfo=KYIV)
        await seed(conn, orders=[
            (oid, src, 19 if is_ret else 1, total, anchor - timedelta(days=days),
             7, manager, None)
            for oid, src, total, days, is_ret, manager in ORDERS])
        await derive(pool)
        await conn.executemany(
            "INSERT INTO app.revenue_goals (period_type,goal_amount,is_custom,"
            "calculated_goal,growth_factor,updated_at) VALUES ($1,$2,$3,$4,$5,now())",
            GOALS)
        await conn.executemany(
            "INSERT INTO app.revenue_predictions (prediction_date, sales_type,"
            " predicted_revenue, model_mae, model_mape, model_wape)"
            " VALUES ($1,$2,$3,$4,$5,$6)", predictions)
    return populate


@pytest_asyncio.fixture
async def in_postgres(tmp_path, monkeypatch):
    """The goals' store, never connected: its DuckDB connection is a trap, so
    an answer below can only be Postgres's."""
    async with tab_in_postgres(DSN, monkeypatch, _populate_with([])):
        yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "/goals")


@pytest_asyncio.fixture
async def with_predictions(tmp_path, monkeypatch):
    """The same, with the stored forecast."""
    async with tab_in_postgres(DSN, monkeypatch, _populate_with(PREDICTIONS)):
        yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "/goals")


GOLDEN = golden("goals")


def _iso(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [_iso(v) for v in value]
    return value


def _literal(name, args=(), kwargs=None):
    """The recorded answer of one call, keyed as the recording keyed it."""
    return GOLDEN[call_key(name, [_iso(list(args)), kwargs or {}])]


async def _answer(store, name, args=(), kwargs=None):
    return comparable(await getattr(store, name)(*args, **(kwargs or {})))


class TestTheHistory:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("period", ["daily", "weekly", "monthly"])
    @pytest.mark.parametrize("sales_type", ["retail", "all", "b2b"])
    async def test_every_grain_and_every_sales_type(self, in_postgres, period, sales_type):
        args, kwargs = (period,), {"weeks_back": 8, "sales_type": sales_type}
        assert await _answer(in_postgres, "get_historical_revenue", args, kwargs) \
            == _literal("get_historical_revenue", args, kwargs)

    @pytest.mark.asyncio
    async def test_the_weekly_trend_branch_runs(self, in_postgres):
        """`trend` is a second statement, reached only when the weekly grain
        found four buckets or more — so it needs its own case, or the branch
        never executes."""
        args, kwargs = ("weekly",), {"weeks_back": 8}
        answer = await _answer(in_postgres, "get_historical_revenue", args, kwargs)
        assert answer == _literal("get_historical_revenue", args, kwargs)
        assert answer["periodCount"] >= 4, (
            "the fixture no longer reaches the trend branch — widen it, or "
            "this test silently stops covering the second statement"
        )


class TestTheDatesGoOverAsDates:
    """The one a mocked `fetch` cannot see: DuckDB coerced an ISO string to a
    DATE, and asyncpg encodes by the type Postgres infers."""

    @pytest.mark.asyncio
    async def test_a_list_of_dates_answers(self, in_postgres):
        dates = [TODAY - timedelta(days=n) for n in (3, 5, 9, 12)]
        answer = await _answer(in_postgres, "get_daily_revenue_for_dates", (dates,))
        assert answer == _literal("get_daily_revenue_for_dates", (dates,))
        assert answer, "no rows came back — the window missed the fixture"

    @pytest.mark.asyncio
    async def test_the_keys_are_dates(self, in_postgres):
        """The caller looks the result up by `date`. A string key would miss
        every lookup and show an empty comparison line rather than an error."""
        dates = [TODAY - timedelta(days=3)]
        result = await in_postgres.get_daily_revenue_for_dates(dates)
        assert comparable(result) == _literal("get_daily_revenue_for_dates", (dates,))
        for key in result:
            assert isinstance(key, date), (key, type(key))

    @pytest.mark.asyncio
    async def test_an_empty_list_asks_nothing(self, in_postgres):
        assert await in_postgres.get_daily_revenue_for_dates([]) == {}
        assert _literal("get_daily_revenue_for_dates", ([],)) == {}


class TestTheStoredGoals:
    @pytest.mark.asyncio
    async def test_get_goals_reads_the_rows(self, in_postgres):
        assert await _answer(in_postgres, "get_goals") == _literal("get_goals")

    @pytest.mark.asyncio
    async def test_is_custom_survives_the_encoding(self, in_postgres):
        """`is_custom` is BOOLEAN in Postgres and came back an integer from
        DuckDB. `get_smart_goals` narrows on it in Python for exactly that
        reason, and the daily goal above is the custom one."""
        goals = await in_postgres.get_goals()
        assert bool(goals["daily"]["isCustom"]) is True
        assert bool(goals["weekly"]["isCustom"]) is False


class TestTheForecast:
    """`get_predictions` is the last read of this tab to move, and it could not
    until revision 0025 gave the table a Postgres home.

    The case that matters is not the numbers — five columns copied verbatim —
    but the **parameters**: the method used to hand its bounds over as ISO
    strings, which DuckDB coerced to DATE and asyncpg refuses. That is
    invisible to a unit test, which mocks `fetch` and never meets the driver.
    """

    @pytest.mark.asyncio
    async def test_a_window_returns_the_rows(self, with_predictions):
        args = (TODAY - timedelta(days=3), TODAY)
        answer = await _answer(with_predictions, "get_predictions", args)
        assert answer == _literal("get_predictions", args)
        assert len(answer) == 3, "retail only, and all three days of it"

    @pytest.mark.asyncio
    async def test_the_sales_type_filter_separates_the_two_rows_of_one_day(
        self, with_predictions,
    ):
        args, kwargs = (TODAY, TODAY), {"sales_type": "b2b"}
        answer = await _answer(with_predictions, "get_predictions", args, kwargs)
        assert answer == _literal("get_predictions", args, kwargs)
        assert len(answer) == 1 and answer[0]["predicted_revenue"] == 12_000.00

    @pytest.mark.asyncio
    async def test_an_empty_window_is_empty(self, with_predictions):
        args = (TODAY - timedelta(days=400), TODAY - timedelta(days=390))
        assert await with_predictions.get_predictions(*args) == []
        assert _literal("get_predictions", args) == []


class TestTheActualToDateJoinsItsPartner:
    """`get_forecast` returns `actual_to_date + predicted_remaining`, and the
    two terms come from one engine through the goal router.

    The case that bites is `sales_type="all"`: Postgres' Gold holds a roll-up
    row and a row per source for every `(date, sales_type)`, so without
    `{gold_revenue_rollup}` the query counts every order twice there. DuckDB's
    Gold, in which the literals were recorded, had one grain and could not.
    """

    @staticmethod
    async def _actual(store, sales_type, start, end):
        from core.prediction_service import PredictionService

        answer = await PredictionService()._get_actual_month_revenue(
            store, sales_type, start, end)
        recorded = GOLDEN[call_key("_get_actual_month_revenue",
                                   [sales_type, start.isoformat(), end.isoformat()])]
        return comparable(answer), recorded

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["retail", "b2b", "all"])
    async def test_every_sales_type_is_the_literal(self, in_postgres, sales_type):
        answer, recorded = await self._actual(
            in_postgres, sales_type, TODAY - timedelta(days=60), TODAY)
        assert answer == recorded

    @pytest.mark.asyncio
    async def test_the_window_is_not_empty_so_the_literal_means_something(
        self, in_postgres,
    ):
        """Zeros pin nothing. The seed lands inside the window, and the b2b
        order is inside 'all' and outside retail."""
        everything, _ = await self._actual(
            in_postgres, "all", TODAY - timedelta(days=60), TODAY)
        retail, _ = await self._actual(
            in_postgres, "retail", TODAY - timedelta(days=60), TODAY)
        assert everything > retail > 0

    @pytest.mark.asyncio
    async def test_an_empty_window_is_zero(self, in_postgres):
        answer, recorded = await self._actual(
            in_postgres, "retail",
            TODAY - timedelta(days=400), TODAY - timedelta(days=390))
        assert answer == recorded == 0.0


class TestTheSmartGoalsForecastSignal:
    """Signal 3 of a smart goal — actual Gold revenue for the month so far plus
    the stored predictions for the rest (DN-12). The actual half is exactly
    the read `_get_actual_month_revenue` is: `{gold_revenue_rollup}` or
    Postgres counts every order twice. `internal` is here because the method
    has always summed every row for any sales_type but retail and b2b, and
    still does.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["retail", "b2b", "all", "internal"])
    async def test_the_actual_part_is_the_literal(self, with_predictions, sales_type):
        args = (TODAY - timedelta(days=60), TODAY, sales_type)
        assert await _answer(with_predictions, "_forecast_actual_revenue", args) \
            == _literal("_forecast_actual_revenue", args)

    @pytest.mark.asyncio
    async def test_the_actual_part_is_money_not_two_zeros(self, with_predictions):
        window = (TODAY - timedelta(days=60), TODAY)
        retail = await with_predictions._forecast_actual_revenue(*window, "retail")
        everything = await with_predictions._forecast_actual_revenue(*window, "all")
        assert retail > 0
        assert everything > retail, "the b2b order must be inside 'all' only"

    @pytest.mark.asyncio
    async def test_the_whole_signal(self, with_predictions):
        """Both halves through one engine. The method swallows errors other
        than a refusal into 0.0, so a broken read shows up as a mismatch
        against an answer that is not zero."""
        args = (TODAY.year, TODAY.month, "retail")
        answer = await _answer(with_predictions, "_get_ml_forecast_total", args)
        assert answer > 0, "no prediction for today — the signal compared nothing"
        assert answer == _literal("_get_ml_forecast_total", args)

        # And what it is, written out: retail Gold from the 1st through
        # yesterday — no return, no retired source 3, no b2b — plus the retail
        # predictions from today to the month's end, on the frozen clock.
        first = TODAY.replace(day=1)
        actual = sum(
            total for _oid, source, total, days_ago, is_return, manager in ORDERS
            if not is_return and source != 3 and manager is None
            and first <= TODAY - timedelta(days=days_ago) < TODAY)
        predicted = sum(
            revenue for day, sales_type, revenue, *_ in PREDICTIONS
            if sales_type == "retail" and TODAY <= day
            and (day.year, day.month) == (TODAY.year, TODAY.month))
        assert answer == round(actual + predicted, 6)


def _frame_literal(df):
    """A training frame in the form the literal was recorded in: columns,
    dtypes, and the rows with each date as its ISO day."""
    rows = [[(v.date().isoformat() if hasattr(v, "date") and hasattr(v, "hour") else v)
             for v in row] for row in df.itertuples(index=False, name=None)]
    return {"columns": list(df.columns),
            "dtypes": {c: str(t) for c, t in df.dtypes.items()},
            "rows": comparable(rows)}


class TestTheForecastTrainingFrame:
    """`_query_daily_revenue` is what the forecast model learns from — training,
    `evaluate`, `tune`, and `predict_range` live on `/trend?include_forecast`.

    The model does not read numbers, it reads a DataFrame, and a type drift does
    not raise: `Decimal` for `float64`, `date` for `datetime64[us]`, `int64` for
    `int32` each quietly change what feature engineering computes. So the frame
    has to come back as DuckDB's did, down to the dtype, and these pin it.

    Verified beyond this file on the production catalogue before the switch:
    the frames matched for retail, all and b2b over 765 days, 51 of 52 model
    features exactly equal (the other is `avg_order_value`, off by one cent in
    5 cells), and a model trained on each frame predicted the next 30 days to
    ₴0.0000 per day.
    """

    @staticmethod
    async def _frame(store, sales_type):
        from core.prediction_service import PredictionService

        frame = await PredictionService()._query_daily_revenue(
            store, sales_type, days_back=120)
        return frame, GOLDEN[call_key("_query_daily_revenue", [sales_type, 120])]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["retail", "b2b", "all"])
    async def test_the_frame_is_the_literal_down_to_the_dtype(self, in_postgres, sales_type):
        frame, recorded = await self._frame(in_postgres, sales_type)
        got = _frame_literal(frame)

        assert got["columns"] == recorded["columns"]
        assert got["dtypes"] == recorded["dtypes"], \
            "a dtype drift changes the model's features without raising"
        assert len(got["rows"]) == len(recorded["rows"]) > 0, (
            f"no {sales_type} rows: the comparison proves nothing")
        aov = recorded["columns"].index("avg_order_value")
        for mine, theirs in zip(sorted(got["rows"]), sorted(recorded["rows"])):
            assert mine[:aov] + mine[aov + 1:] == theirs[:aov] + theirs[aov + 1:]
            # DuckDB promoted DECIMAL division to DOUBLE; documented cent.
            assert abs(mine[aov] - theirs[aov]) <= 0.0100001

    @pytest.mark.asyncio
    async def test_all_is_one_row_per_date_and_sales_type_not_per_date(self, in_postgres):
        """DuckDB's Gold key was `(date, sales_type)`, so "all" returns a row
        per type on a day both sold. A `GROUP BY date` would collapse them and
        hand the model different history. On production that is 1,701 rows
        over 765 days, not 765."""
        frame, recorded = await self._frame(in_postgres, "all")
        assert len(frame) == len(recorded["rows"])
        assert len(frame) >= frame["date"].nunique()


def test_normalise_frame_produces_duckdbs_dtypes():
    """Pure: the dtype contract, without a database. Every column the model
    reads, in the types `fetchdf()` produced from the production catalogue."""
    from datetime import date as _date
    from decimal import Decimal
    from core.pg_forecast_read import COLUMNS, normalise_frame

    row = (_date(2026, 8, 1), Decimal("1200.50"), Decimal("600"), Decimal("0"),
           Decimal("600.5"), 3, 3, 1, 2, 0, Decimal("0"), 1, 0, 2, Decimal("400.17"))
    df = normalise_frame([row])
    expected = {"date": "datetime64[us]"}
    expected.update({c: "float64" for c in (
        "revenue", "instagram_revenue", "telegram_revenue", "shopify_revenue",
        "returns_revenue", "avg_order_value")})
    expected.update({c: "int32" for c in (
        "orders_count", "unique_customers", "new_customers", "returning_customers",
        "returns_count", "instagram_orders", "telegram_orders", "shopify_orders")})
    assert list(df.columns) == list(COLUMNS)
    assert dict(df.dtypes.astype(str)) == expected


def test_every_literal_is_asked():
    """The literal holds the calls above and nothing they do not ask."""
    asked = {k.split("[", 1)[0] for k in GOLDEN}
    assert asked == {
        "get_historical_revenue", "get_daily_revenue_for_dates", "get_goals",
        "get_predictions", "_get_actual_month_revenue", "_forecast_actual_revenue",
        "_get_ml_forecast_total", "_query_daily_revenue"}
