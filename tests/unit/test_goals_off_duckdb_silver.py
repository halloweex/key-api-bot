"""The goal calculators stop reading DuckDB Silver (DN-12), and read Silver.

Seasonality, YoY, weekly patterns, the growth cap and the two history reads of
`generate_smart_goals` narrowed `orders` through an EXISTS over DuckDB
`silver_orders`. Silver stops moving at step 13, after which that EXISTS drops
every newer order; the next compaction empties it, the EXISTS matches nothing,
and `calculate_yoy_growth` wrote its 0.10 placeholder over the measured rate —
which the hourly full replace carried into Postgres.

DN-12 replaced it with a bridge — `silver_sales_type_case` rendered over
DuckDB `orders` — and chain 7b-2 with `{silver_orders}` through the goal
router, whose engine is `KS_READ_GOALS`'. Chain 7b-4 deleted the bridge, and
with it the tests that compared it against the Silver EXISTS, its compaction
shape and the tripwire that held chains 3 and 5 while it was rendered. What
stays here proves the Silver history and what never depended on the bridge:

  * **the shared fixtures** — the equivalence fixture (one manager per branch
    of the sales-type CASE) and the three-year history;
  * **empty history** — the placeholder never overwrites a stored rate;
  * **the forecast signal**, **what the smart goal narrows to**, and **every
    calculator** — seasonality, YoY yearly and per month, the weekly weights
    and the cap, for every sales type — against numbers written out here, in
    Python. The last replaces the comparison with the bridge's own bodies,
    which was the only independent answer the Silver bodies had.

`tests/unit/test_goals_history_silver.py` holds the Silver selection and the
retired switch; the two-engine half is in
`tests/integration/test_goals_history_two_engines.py`.
"""
from __future__ import annotations

import asyncio
import logging
import math
import statistics
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, patch

from core.duckdb_constants import (
    B2B_MANAGER_ID,
    EXHIBITION_SOURCE_ID,
    KNOWN_SALES_TYPES,
    RETAIL_MANAGER_IDS,
)
from core.duckdb_store import DuckDBStore
from core.models import OrderStatus

KYIV = ZoneInfo("Europe/Kyiv")
TIMEOUT_S = 60


# ─── The equivalence fixture ────────────────────────────────────────────────
#
# One manager per branch of `silver_sales_type_case`, and one that tells the
# last two fallbacks apart.

RETAIL = RETAIL_MANAGER_IDS[0]          # listed, classified retail
NEVER_CLASSIFIED = RETAIL_MANAGER_IDS[1]  # listed, is_retail, no interval
LISTED_BUT_NOT_RETAIL = RETAIL_MANAGER_IDS[2]  # listed, is_retail FALSE, no interval
AS_OF = 34                               # retail from SWITCH on, internal before
UNLISTED_RETAIL = 28                     # is_retail TRUE, not listed, no interval
INTERNAL = 40
SWITCH = date(2026, 3, 1)

assert AS_OF not in RETAIL_MANAGER_IDS
assert UNLISTED_RETAIL not in RETAIL_MANAGER_IDS
assert INTERNAL not in RETAIL_MANAGER_IDS
assert B2B_MANAGER_ID not in RETAIL_MANAGER_IDS

MANAGERS = [  # (id, is_retail)
    (RETAIL, True), (NEVER_CLASSIFIED, True), (LISTED_BUT_NOT_RETAIL, False),
    (AS_OF, False), (UNLISTED_RETAIL, True), (INTERNAL, False),
    (B2B_MANAGER_ID, False),
]
INTERVALS = [  # (manager_id, is_retail, valid_from, valid_to)
    (RETAIL, True, date(1970, 1, 1), None),
    (AS_OF, False, date(1970, 1, 1), SWITCH),
    (AS_OF, True, SWITCH, None),
    (INTERNAL, False, date(1970, 1, 1), None),
    (B2B_MANAGER_ID, False, date(1970, 1, 1), None),
]


def _kyiv_noon(d: date) -> datetime:
    return datetime.combine(d, time(12, 0), tzinfo=KYIV)


# (id, source_id, status_id, manager_id, ordered_at)
ORDERS = [
    (1, 1, 1, None, _kyiv_noon(date(2026, 2, 10))),
    (2, 1, 1, B2B_MANAGER_ID, _kyiv_noon(date(2026, 2, 10))),
    (3, EXHIBITION_SOURCE_ID, 1, RETAIL, _kyiv_noon(date(2026, 2, 11))),
    (4, 1, 1, RETAIL, _kyiv_noon(date(2026, 2, 11))),
    (5, 2, 1, AS_OF, _kyiv_noon(SWITCH - timedelta(days=5))),
    (6, 2, 1, AS_OF, _kyiv_noon(SWITCH + timedelta(days=5))),
    (7, 2, 1, NEVER_CLASSIFIED, _kyiv_noon(date(2026, 2, 12))),
    (8, 1, 1, INTERNAL, _kyiv_noon(date(2026, 2, 12))),
    (9, EXHIBITION_SOURCE_ID, 1, None, _kyiv_noon(date(2026, 2, 13))),
    (10, EXHIBITION_SOURCE_ID, 1, B2B_MANAGER_ID, _kyiv_noon(date(2026, 2, 13))),
    (11, 4, 19, None, _kyiv_noon(date(2026, 2, 14))),   # a return: status is not this rule
    # 22:30 Kyiv on the eve of SWITCH is 20:30 UTC the same day; 00:30 Kyiv
    # on SWITCH is 22:30 UTC the day before. Both sides must date it by Kyiv.
    (12, 1, 1, AS_OF, datetime.combine(SWITCH, time(0, 30), tzinfo=KYIV)),
    (13, 1, 1, LISTED_BUT_NOT_RETAIL, _kyiv_noon(date(2026, 2, 15))),
    (14, 1, 1, UNLISTED_RETAIL, _kyiv_noon(date(2026, 2, 15))),
    (15, 1, 1, AS_OF, datetime.combine(SWITCH - timedelta(days=1), time(23, 30), tzinfo=KYIV)),
]

ALL_IDS = {o[0] for o in ORDERS}
EXHIBITION = {3, 9, 10}

# What each scenario must select, written out rather than derived, so that a
# rule changed in the CASE itself — and therefore in Silver too — still fails.
EXPECTED = {
    "classified": {
        "retail": {1, 4, 6, 7, 11, 12, 14},
        "b2b": {2},
        "exhibition": EXHIBITION,
        "internal": {5, 8, 13, 15},
    },
    # No intervals at all: `managers.is_retail` decides.
    "no_classifications": {
        "retail": {1, 4, 7, 11, 14},
        "b2b": {2},
        "exhibition": EXHIBITION,
        "internal": {5, 6, 8, 12, 13, 15},
    },
    # Neither table has spoken: the listed ids decide.
    "nothing_synced": {
        "retail": {1, 4, 7, 11, 13},
        "b2b": {2},
        "exhibition": EXHIBITION,
        "internal": {5, 6, 8, 12, 14, 15},
    },
}


async def _seed_equivalence(store, scenario: str) -> None:
    async with store.connection() as conn:
        conn.execute("DELETE FROM manager_classifications")
        conn.execute("DELETE FROM managers")
        if scenario != "nothing_synced":
            conn.executemany(
                "INSERT INTO managers (id, name, is_retail) VALUES (?, 'm', ?)",
                MANAGERS)
        if scenario == "classified":
            conn.executemany(
                "INSERT INTO manager_classifications "
                "(manager_id, is_retail, valid_from, valid_to) VALUES (?, ?, ?, ?)",
                INTERVALS)
        conn.executemany(
            "INSERT INTO orders (id, source_id, status_id, grand_total, "
            "ordered_at, buyer_id, manager_id) VALUES (?, ?, ?, 1000.0, ?, 1, ?)",
            [(oid, src, st, when, mgr) for oid, src, st, mgr, when in ORDERS])
    await store.refresh_warehouse_layers(trigger="manual")


@pytest_asyncio.fixture
async def store(tmp_path):
    s = DuckDBStore(db_path=tmp_path / "goals.duckdb")
    await s.connect()
    try:
        yield s
    finally:
        await s.close()


class TestTheSummaryFallbackReadsTheRowsOwnSalesType:
    """`get_summary_stats`' DuckDB branch for one source counts that source's
    returns from `silver_orders` — through an EXISTS back into `silver_orders`
    for the same id until DN-12, now through the row's own column. Same row,
    same answer, and an unknown sales_type still refuses."""

    WINDOW = (date(2026, 2, 1), date(2026, 3, 31))

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type,returns", [
        ("retail", 1), ("b2b", 0), ("internal", 0), ("all", 1)])
    async def test_the_returns_of_one_source(self, store, monkeypatch, sales_type, returns):
        monkeypatch.delenv("KS_READ_GOLD", raising=False)
        monkeypatch.delenv("KS_READ_SILVER", raising=False)
        await _seed_equivalence(store, "classified")
        out = await store.get_summary_stats(*self.WINDOW, source_id=4,
                                            sales_type=sales_type)
        assert out["totalReturns"] == returns
        assert out["returnsRevenue"] == 1000.0 * returns

    @pytest.mark.asyncio
    async def test_an_unknown_sales_type_still_refuses(self, store, monkeypatch):
        monkeypatch.delenv("KS_READ_GOLD", raising=False)
        await _seed_equivalence(store, "classified")
        with pytest.raises(ValueError):
            await store.get_summary_stats(*self.WINDOW, source_id=4,
                                          sales_type="wholsale")


# ─── Three years of history, for the calculators ───────────────────────────

# Production's shape: the first order is 2023-12-02, so there are two full
# years and one pair of them. (Three full years reached a separate, older
# defect — `yoy_rates` held DuckDB DECIMALs and the recency weights are floats,
# so a second pair raised TypeError. Chain 7b-1 fixed it; the pinning is in
# `tests/unit/test_goals_read_paths_do_not_write.py`.)
HISTORY_START = date(2023, 12, 2)
HISTORY_END = date(2025, 12, 31)


def _history_rows():
    """One retail order a day with a seasonal and a yearly shape, plus a b2b
    order on eight days in nine (enough for b2b to reach every month's 20-day
    floor), an exhibition order every seventh (by a retail manager — the case
    the old filter got wrong) and a return every eleventh."""
    rows, oid, d = [], 1, HISTORY_START
    while d <= HISTORY_END:
        growth = 1.0 + 0.15 * (d.year - HISTORY_START.year)
        season = 1.0 + 0.4 * (d.month in (11, 12)) - 0.2 * (d.month in (6, 7))
        total = round(1000.0 * growth * season + (d.day % 7) * 13.0, 2)
        rows.append((oid, 1, 1, total, _kyiv_noon(d), None)); oid += 1
        if d.toordinal() % 9 != 0:
            rows.append((oid, 1, 1, 500.0 + d.month * 11.0, _kyiv_noon(d), B2B_MANAGER_ID)); oid += 1
        if d.toordinal() % 7 == 0:
            rows.append((oid, EXHIBITION_SOURCE_ID, 1, 700.0, _kyiv_noon(d), RETAIL)); oid += 1
        if d.toordinal() % 11 == 0:
            rows.append((oid, 2, 19, 900.0, _kyiv_noon(d), None)); oid += 1
        d += timedelta(days=1)
    return rows


async def _seed_history(store) -> None:
    async with store.connection() as conn:
        conn.execute("DELETE FROM manager_classifications")
        conn.execute("DELETE FROM managers")
        conn.executemany(
            "INSERT INTO managers (id, name, is_retail) VALUES (?, 'm', ?)",
            [(RETAIL, True), (B2B_MANAGER_ID, False)])
        conn.executemany(
            "INSERT INTO manager_classifications "
            "(manager_id, is_retail, valid_from, valid_to) VALUES (?, ?, ?, NULL)",
            [(RETAIL, True, date(1970, 1, 1)), (B2B_MANAGER_ID, False, date(1970, 1, 1))])
        conn.executemany(
            "INSERT INTO orders (id, source_id, status_id, grand_total, "
            "ordered_at, buyer_id, manager_id) VALUES (?, ?, ?, ?, ?, 1, ?)",
            _history_rows())
        conn.execute("DELETE FROM seasonal_indices")
        conn.execute("DELETE FROM weekly_patterns")
        conn.execute("DELETE FROM growth_metrics")
    await store.refresh_warehouse_layers(trigger="manual")


def _settled(value):
    """Floats to twelve places. DuckDB aggregates DOUBLEs in whatever order the
    plan hands it rows, so AVG/STDDEV over the same values can differ in the
    last bit between two plans — 0.21506006243397469 against …463 was seen for
    the growth cap. That is the engine, not the answer."""
    if isinstance(value, dict):
        return {k: _settled(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_settled(v) for v in value]
    if isinstance(value, float) or hasattr(value, "as_tuple"):
        return round(float(value), 12)
    return value


async def _calculators(store, sales_type):
    seasonality = await store.calculate_seasonality_indices(sales_type)
    yoy = await store.calculate_yoy_growth(sales_type)
    weekly = await store.calculate_weekly_patterns(sales_type)
    caps = [await store._dynamic_growth_cap(m, sales_type) for m in range(1, 13)]
    return _settled([seasonality, yoy, weekly, caps])


def _without_clock(smart):
    smart = dict(smart)
    smart["metadata"] = {k: v for k, v in smart["metadata"].items()
                         if k != "calculatedAt"}
    return _settled(smart)


async def _smart(store, sales_type):
    """Next month's smart goal, after storing the three tables the way
    `POST /goals/recalculate` does — `generate_smart_goals` reads only since
    chain 7b-1 (OD-14 (i)). Bounded: if a history read were taken inside the
    store lock again, this would hang rather than fail."""
    await asyncio.wait_for(
        store.recalculate_goal_tables(include_weekly=True), TIMEOUT_S)
    return _without_clock(await asyncio.wait_for(
        store.generate_smart_goals(2026, 10, sales_type), TIMEOUT_S))


class TestAnEmptyHistoryLeavesTheStoredRateAlone:
    STORED = 0.2345

    async def _stored_yoy(self, store):
        async with store.connection() as conn:
            row = conn.execute(
                "SELECT value, sample_size FROM growth_metrics "
                "WHERE metric_type = 'yoy_overall'").fetchone()
        return None if row is None else (float(row[0]), row[1])

    # Through `recalculate_goal_tables`, the one writer since chain 7b-1: the
    # calculator itself stores nothing any more, so asking it would pass the
    # first of these by writing nothing at all.

    @pytest.mark.asyncio
    async def test_the_placeholder_does_not_overwrite_a_measured_rate(self, store):
        async with store.connection() as conn:
            conn.execute("DELETE FROM growth_metrics")
            conn.execute(
                "INSERT INTO growth_metrics (metric_type, value, sample_size) "
                "VALUES ('yoy_overall', ?, 3)", [self.STORED])

        tables = await store.recalculate_goal_tables(include_weekly=False)
        result = tables["yoy"]

        assert result["sample_size"] == 0 and result["overall_yoy"] == 0.10, (
            "the returned value is unchanged — only what is persisted is")
        assert await self._stored_yoy(store) == (self.STORED, 3)

    @pytest.mark.asyncio
    async def test_a_first_run_still_writes_the_placeholder(self, store):
        """Nothing better to keep: behaviour before DN-12, unchanged."""
        async with store.connection() as conn:
            conn.execute("DELETE FROM growth_metrics")
        await store.recalculate_goal_tables(include_weekly=False)
        assert await self._stored_yoy(store) == (0.10, 0)

    @pytest.mark.asyncio
    async def test_a_real_history_still_overwrites(self, store):
        await _seed_history(store)
        async with store.connection() as conn:
            conn.execute(
                "INSERT INTO growth_metrics (metric_type, value, sample_size) "
                "VALUES ('yoy_overall', ?, 3)", [self.STORED])
        result = (await store.recalculate_goal_tables(include_weekly=False))["yoy"]
        stored = await self._stored_yoy(store)
        assert result["sample_size"] >= 1
        assert stored[0] == pytest.approx(float(result["overall_yoy"]), abs=1e-4)
        assert stored[0] != self.STORED


# ─── The forecast signal ────────────────────────────────────────────────────
#
# Signal 3 of a smart goal: Gold for the days of the month already gone, plus
# the stored predictions from today to the month's end. The two-engine test
# proves both engines agree; this proves what they agree *on* — the bounds are
# computed in Python, so a date moved by one day moves both engines together.

FORECAST_TODAY = date(2025, 3, 15)


class _FrozenClock(datetime):
    """`datetime` with `now()` pinned to midday in Kyiv on FORECAST_TODAY."""

    @classmethod
    def now(cls, tz=None):
        moment = datetime.combine(FORECAST_TODAY, time(12, 0), tzinfo=KYIV)
        return moment.astimezone(tz) if tz else moment.replace(tzinfo=None)


# (id, grand_total, day, manager_id) — all source 1, none a return.
FORECAST_ORDERS = [
    (1, 7000.0, date(2025, 2, 28), None),            # last month
    (2, 1000.0, date(2025, 3, 1), None),             # the first
    (3, 300.0, date(2025, 3, 5), B2B_MANAGER_ID),    # b2b: inside 'all' only
    (4, 200.0, date(2025, 3, 14), None),             # yesterday: the actual half's last day
    (5, 5000.0, date(2025, 3, 15), None),            # today: the predicted half's
]
FORECAST_PREDICTIONS = [  # (prediction_date, sales_type, predicted_revenue)
    (date(2025, 3, 10), "retail", 40_000.0),   # already happened — Gold answers for it
    (date(2025, 3, 15), "retail", 100_000.0),  # today
    (date(2025, 3, 31), "retail", 20_000.0),   # the month's last day
    (date(2025, 4, 1), "retail", 9_000.0),     # next month
    (date(2025, 3, 20), "b2b", 12_345.0),
    (date(2025, 3, 20), "all", 50_000.0),
]
EXPECTED_SIGNAL = {
    "retail": 1000.0 + 200.0 + 100_000.0 + 20_000.0,
    "all": 1000.0 + 300.0 + 200.0 + 50_000.0,
}


async def _seed_forecast(store) -> None:
    async with store.connection() as conn:
        conn.executemany(
            "INSERT INTO orders (id, source_id, status_id, grand_total, "
            "ordered_at, buyer_id, manager_id) VALUES (?, 1, 1, ?, ?, 1, ?)",
            [(oid, total, _kyiv_noon(day), mgr)
             for oid, total, day, mgr in FORECAST_ORDERS])
        conn.executemany(
            "INSERT INTO revenue_predictions (prediction_date, sales_type, "
            "predicted_revenue, model_mae, model_mape, model_wape) "
            "VALUES (?, ?, ?, 1, 1, 1)", FORECAST_PREDICTIONS)
    await store.refresh_warehouse_layers(trigger="manual")
    async with store.connection() as conn:
        gold = conn.execute(
            "SELECT COALESCE(SUM(revenue), 0) FROM gold_daily_revenue "
            "WHERE date BETWEEN '2025-03-01' AND '2025-03-31'").fetchone()[0]
    assert float(gold) == 6500.0, "Gold was not built from the seed — nothing is measured"


class TestTheForecastSignal:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", sorted(EXPECTED_SIGNAL))
    async def test_gold_through_yesterday_plus_predictions_from_today(
        self, store, monkeypatch, sales_type,
    ):
        """Today's order belongs to the predicted half and the 10th's
        prediction to the actual one — each counted once, in its own half."""
        monkeypatch.delenv("KS_READ_GOALS", raising=False)
        await _seed_forecast(store)
        with patch("core.repositories.goals.datetime", _FrozenClock):
            got = await store._get_ml_forecast_total(
                FORECAST_TODAY.year, FORECAST_TODAY.month, sales_type)
        assert got == pytest.approx(EXPECTED_SIGNAL[sales_type], abs=0.005)

    @pytest.mark.asyncio
    async def test_a_read_that_fails_is_left_out_and_said_at_warning(
        self, store, monkeypatch, caplog,
    ):
        """The goal is then computed from the other two signals. At DEBUG
        nothing said so; that is a different goal nobody was told about."""
        monkeypatch.delenv("KS_READ_GOALS", raising=False)
        caplog.set_level(logging.DEBUG, logger="core.repositories.goals")
        with patch.object(type(store), "_goals_run",
                          new=AsyncMock(side_effect=RuntimeError("duckdb is gone"))):
            got = await store._get_ml_forecast_total(2025, 3, "retail")
        assert got == 0.0
        warned = [r for r in caplog.records
                  if r.name == "core.repositories.goals"
                  and r.levelno == logging.WARNING]
        assert warned and "duckdb is gone" in warned[0].getMessage()

    @pytest.mark.asyncio
    async def test_a_fallback_on_one_half_mixes_the_engines(
        self, store, monkeypatch, caplog,
    ):
        """What the comment on `_FORECAST_ACTUAL_SQL` says, pinned. The
        fallback is per read, so a Postgres failure on the actual half is
        answered by DuckDB while the predicted half is still Postgres' — the
        sum is not 0.0, it mixes engines, and `_goals_run` says so at ERROR.
        `PredictionService.get_forecast` accepts the same; DN-20's off mode is
        what turns it into a refusal."""
        await _seed_forecast(store)
        monkeypatch.setenv("KS_READ_GOALS", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")

        async def fetch(sql, params=()):
            if "gold.daily_revenue" in sql:
                raise ConnectionError("postgres failed on the actual half")
            return [(1000.0,)]

        caplog.set_level(logging.ERROR, logger="core.repositories.goals")
        with patch("core.pg_goals_read.fetch", new=fetch), \
             patch("core.repositories.goals.datetime", _FrozenClock):
            got = await store._get_ml_forecast_total(
                FORECAST_TODAY.year, FORECAST_TODAY.month, "retail")
        assert got == pytest.approx(1000.0 + 200.0 + 1000.0, abs=0.005), (
            "DuckDB's actual half (1 000 + 200) and Postgres' predicted one")
        assert any("falling back to DuckDB" in r.getMessage()
                   for r in caplog.records if r.levelno == logging.ERROR)


# ─── What the smart goal narrows to, in written-out numbers ─────────────────
#
# A comparison of one history against another passes when both lose a filter
# — DN-12's review found three sites that did (mutations x1, x2 and cap). These
# expect numbers computed here, in Python, from the rows as `_history_rows`
# wrote them, so a Silver body that stops narrowing fails on its own.

RETURN_STATUSES = frozenset(int(s) for s in OrderStatus.return_statuses())


def _history_sales_type(source_id, manager_id):
    """The fixture's rows, typed by how they were written, not by the CASE."""
    if source_id == EXHIBITION_SOURCE_ID:
        return "exhibition"
    return "b2b" if manager_id == B2B_MANAGER_ID else "retail"


def _history_days(sales_type):
    """`(Kyiv date, total)` for every order the history counts, returns left
    out; `None` is every sales type — what a read that lost its filter sees,
    and what `'all'` asks for."""
    for _oid, src, status, total, when, mgr in _history_rows():
        if status in RETURN_STATUSES:
            continue
        if sales_type is not None and _history_sales_type(src, mgr) != sales_type:
            continue
        # `when` is Kyiv-aware, so this is the Kyiv date.
        yield when.date(), Decimal(str(total))


def _monthly_history(sales_type):
    """`{(year, month): (revenue, days with orders)}`, returns left out;
    `None` is every sales type — what a read that lost its filter sees."""
    revenue, days = defaultdict(Decimal), defaultdict(set)
    for day, total in _history_days(sales_type):
        revenue[(day.year, day.month)] += total
        days[(day.year, day.month)].add(day)
    return {k: (float(revenue[k]), len(days[k])) for k in revenue}


def _expected_last_year(sales_type, year, month):
    return _monthly_history(sales_type).get((year - 1, month), (0.0, 0))[0]


def _expected_recent_average(sales_type):
    """The last three complete months with 25 days of orders or more. The
    fixture ends in 2025, so every month of it is complete."""
    history = _monthly_history(sales_type)
    months = sorted((k for k, (_r, n) in history.items() if n >= 25),
                    reverse=True)[:3]
    return sum(history[k][0] for k in months) / len(months)


def _expected_cap(sales_type, month):
    """avg + 1.5 * sample stddev of consecutive-year YoY over years with 25
    days of orders in `month`, clamped to [0.10, 0.50]; 0.35 with no pair."""
    by_year = {y: r for (y, m), (r, n) in _monthly_history(sales_type).items()
               if m == month and n >= 25}
    rates = [(by_year[y] - by_year[y - 1]) / by_year[y - 1]
             for y in sorted(by_year) if by_year.get(y - 1)]
    if not rates:
        return 0.35
    spread = statistics.stdev(rates) if len(rates) > 1 else 0.0
    return max(0.10, min(0.50, statistics.mean(rates) + 1.5 * spread))


class TestTheSmartGoalNarrowsToItsOwnSalesType:
    TARGET = (2026, 10)

    def test_the_fixture_tells_a_narrowed_read_from_one_that_is_not(self):
        """Otherwise the numbers below would agree with a read that lost its
        filter, and prove nothing."""
        year, month = self.TARGET
        for sales_type in ("retail", "b2b"):
            assert _expected_last_year(sales_type, year, month) \
                != _expected_last_year(None, year, month)
            assert _expected_recent_average(sales_type) \
                != _expected_recent_average(None)
        assert _expected_cap("retail", month) != _expected_cap(None, month)
        assert 0.10 < _expected_cap("retail", month) < 0.50, (
            "a clamped cap cannot show a filter being lost")

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["retail", "b2b"])
    async def test_last_year_the_recent_months_and_the_cap(
        self, store, sales_type,
    ):
        """Numbers written out here, so a Silver body that lost its filter
        fails as a bridge one did (mutations x1, x2 and cap)."""
        await _seed_history(store)
        monthly = (await _smart(store, sales_type))["monthly"]
        year, month = self.TARGET
        assert monthly["lastYearRevenue"] == pytest.approx(
            _expected_last_year(sales_type, year, month), abs=0.005)
        assert monthly["recent3MonthAvg"] == pytest.approx(
            _expected_recent_average(sales_type), abs=0.005)
        assert monthly["growthCap"] == pytest.approx(
            _expected_cap(sales_type, month), abs=1e-4)

    @pytest.mark.asyncio
    async def test_the_cap_for_every_month(self, store):
        """December has three years in the fixture and so two pairs — the one
        month where the standard deviation is not zero."""
        await _seed_history(store)
        caps = [await store._dynamic_growth_cap(m, "retail")
                for m in range(1, 13)]
        assert caps == pytest.approx(
            [_expected_cap("retail", m) for m in range(1, 13)], rel=1e-9)


# ─── Every calculator, in written-out numbers ───────────────────────────────
#
# Until chain 7b-4 the Silver bodies of seasonality, YoY and the weekly
# weights were held to the bridge's separately written bodies
# (`TestTheAnswersAreTheSame`). The bridge is gone, and the two-engine test
# runs one text on two engines, so a mistake in that text answers alike on
# both. Here the same arithmetic is done in Python from the rows
# `_history_rows` wrote — what `_expected_cap` already did for the cap, done
# for the rest — so a Silver body that computes the wrong thing fails on its
# own. Review mutations (7b-4): MS2 `MIN(revenue) AS max_revenue`, MS3 the
# monthly YoY divided by `curr.revenue`, MS5 the index over `max_revenue`,
# and `LEAST(4, …)` in the weekly body.

MONEY = 0.0101     # the answer rounds to the cent
RATIO = 1.01e-4    # and these to four places

# The weights a month or week without orders is filled with, written out
# rather than imported from the module under test.
DEFAULT_WEEKLY_WEIGHTS = {1: 0.23, 2: 0.23, 3: 0.23, 4: 0.23, 5: 0.08}


def _history_filter(sales_type):
    """A sales type as `_history_days` takes it: `'all'` is every one."""
    return None if sales_type == "all" else sales_type


def _weighted_by_recency(rates):
    """The oldest pair weighs 1.0 and the newest 2.0, linear between."""
    if len(rates) == 1:
        return rates[0]
    weights = [1.0 + i / (len(rates) - 1) for i in range(len(rates))]
    return sum(r * w for r, w in zip(rates, weights)) / sum(weights)


def _expected_seasonality(sales_type):
    """Per calendar month, over the years in which it had 20 days of orders
    or more: the mean, the least and the greatest year, how many, their
    sample deviation, and the mean over the mean of every month's mean."""
    by_month = defaultdict(list)
    history = _monthly_history(_history_filter(sales_type))
    for (_year, month), (revenue, days) in sorted(history.items()):
        if days >= 20:
            by_month[month].append(revenue)
    if not by_month:
        return {}
    means = {month: statistics.mean(r) for month, r in by_month.items()}
    grand = statistics.mean(means.values())
    out = {}
    for month, revenues in sorted(by_month.items()):
        n = len(revenues)
        out[month] = {
            "month": month,
            "avg_revenue": means[month],
            "min_revenue": min(revenues),
            "max_revenue": max(revenues),
            "sample_size": n,
            "std_dev": statistics.stdev(revenues) if n > 1 else 0.0,
            "seasonality_index": means[month] / grand,
            "confidence": "high" if n >= 3 else "medium" if n >= 2 else "low",
        }
    return out


def _expected_yoy(sales_type):
    """Full years — eleven months with orders or more — paired in order, and
    per calendar month the consecutive years with 25 days of orders each,
    every rate `(this - last) / last`; both weighted by recency. The fixture
    ends before the running Kyiv year, so no year is left out for that."""
    history = _monthly_history(_history_filter(sales_type))
    revenue, months = defaultdict(Decimal), defaultdict(set)
    for (year, month), (month_revenue, _days) in history.items():
        revenue[year] += Decimal(str(month_revenue))
        months[year].add(month)
    full = sorted(year for year in revenue if len(months[year]) >= 11)
    rates = [float((revenue[b] - revenue[a]) / revenue[a])
             for a, b in zip(full, full[1:]) if revenue[a] > 0]
    monthly = {}
    for month in range(1, 13):
        by_year = {y: r for (y, m), (r, n) in history.items()
                   if m == month and n >= 25}
        pairs = [(by_year[y] - by_year[y - 1]) / by_year[y - 1]
                 for y in sorted(by_year) if by_year.get(y - 1)]
        if pairs:
            monthly[month] = _weighted_by_recency(pairs)
    return {
        "overall_yoy": _weighted_by_recency(rates) if rates else 0.10,
        "monthly_yoy": monthly,
        "yearly_data": [{"year": y, "revenue": float(revenue[y])} for y in full],
        "sample_size": len(rates),
    }


def _expected_weekly(sales_type):
    """Week of month 1-5 by day (`ceil(day / 7)`, the 29th to the 31st in the
    fifth): each week's share of its month, averaged over the months it
    occurs in; anything unmeasured is the default."""
    weekly = defaultdict(Decimal)
    for day, total in _history_days(_history_filter(sales_type)):
        weekly[(day.year, day.month, min(5, math.ceil(day.day / 7)))] += total
    totals = defaultdict(Decimal)
    for (year, month, _week), revenue in weekly.items():
        totals[(year, month)] += revenue
    shares = defaultdict(list)
    for (year, month, week), revenue in weekly.items():
        shares[(month, week)].append(revenue / totals[(year, month)])
    out = {month: dict(DEFAULT_WEEKLY_WEIGHTS) for month in range(1, 13)}
    for (month, week), share in shares.items():
        out[month][week] = float(sum(share) / len(share))
    return out


class TestEveryCalculatorInWrittenOutNumbers:
    """Seasonality, YoY (yearly and per month), the weekly weights and the
    cap, for every sales type, against the numbers above. What the Monday job
    and the POST store is these answers column by column
    (`test_goals_read_paths_do_not_write.py::TestWhatIsStored`), so the
    stored rows are pinned through them."""

    def test_the_fixture_tells_each_mutation_apart(self):
        """Otherwise the numbers below would agree with a body that made the
        mistake, and prove nothing."""
        assert HISTORY_END.year < date.today().year, (
            "the running Kyiv year would drop a year from the yearly read")
        for sales_type in ("retail", "b2b", "all"):
            # Retail grows every year, so every month tells; b2b's amount is
            # fixed per month, so only the months whose day counts differ do.
            every = all if sales_type == "retail" else any
            seasonal = _expected_seasonality(sales_type)
            assert len(seasonal) == 12, sales_type
            # MS2 and MS5: a month with two years that differ.
            assert every(m["sample_size"] >= 2 and m["min_revenue"] < m["max_revenue"]
                         and m["avg_revenue"] < m["max_revenue"]
                         for m in seasonal.values()), sales_type
            # MS3: dividing by this year instead of the last moves the rate.
            assert every(abs(rate) > 0.01 for rate
                         in _expected_yoy(sales_type)["monthly_yoy"].values()), sales_type
            # LEAST(4, …): the 29th to the 31st are a week of their own.
            assert abs(_expected_weekly(sales_type)[1][5]
                       - DEFAULT_WEEKLY_WEIGHTS[5]) > 0.01, sales_type
        retail = _expected_yoy("retail")
        assert len(retail["monthly_yoy"]) == 12 and retail["sample_size"] == 1
        # December has three years, so two pairs: the recency weighting shows.
        assert sorted(y for (y, m), (_r, n) in _monthly_history("retail").items()
                      if m == 12 and n >= 25) == [2023, 2024, 2025]
        assert _expected_seasonality("internal") == {}, "no internal orders"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", [*KNOWN_SALES_TYPES, "all"])
    async def test_the_calculators(self, store, sales_type):
        await _seed_history(store)
        seasonal = await store.calculate_seasonality_indices(sales_type)
        yoy = await store.calculate_yoy_growth(sales_type)
        weekly = await store.calculate_weekly_patterns(sales_type)
        caps = [await store._dynamic_growth_cap(month, sales_type)
                for month in range(1, 13)]

        want = _expected_seasonality(sales_type)
        assert set(seasonal) == set(want), sales_type
        for month, row in want.items():
            got = seasonal[month]
            for key in ("avg_revenue", "min_revenue", "max_revenue", "std_dev"):
                assert got[key] == pytest.approx(row[key], abs=MONEY), (
                    sales_type, month, key)
            assert got["seasonality_index"] == pytest.approx(
                row["seasonality_index"], abs=RATIO), (sales_type, month)
            assert (got["month"], got["sample_size"], got["confidence"]) == (
                month, row["sample_size"], row["confidence"]), (sales_type, month)

        want_yoy = _expected_yoy(sales_type)
        assert yoy["sample_size"] == want_yoy["sample_size"], sales_type
        assert yoy["overall_yoy"] == pytest.approx(want_yoy["overall_yoy"], abs=RATIO)
        assert [y["year"] for y in yoy["yearly_data"]] == [
            y["year"] for y in want_yoy["yearly_data"]], sales_type
        assert [y["revenue"] for y in yoy["yearly_data"]] == pytest.approx(
            [y["revenue"] for y in want_yoy["yearly_data"]], abs=MONEY)
        assert set(yoy["monthly_yoy"]) == set(want_yoy["monthly_yoy"]), sales_type
        for month, rate in want_yoy["monthly_yoy"].items():
            assert yoy["monthly_yoy"][month] == pytest.approx(rate, abs=RATIO), (
                sales_type, month)

        want_weekly = _expected_weekly(sales_type)
        assert {m: set(w) for m, w in weekly.items()} == {
            m: set(w) for m, w in want_weekly.items()}, sales_type
        for month, weeks in want_weekly.items():
            for week, weight in weeks.items():
                assert weekly[month][week] == pytest.approx(weight, abs=RATIO), (
                    sales_type, month, week)

        assert caps == pytest.approx(
            [_expected_cap(_history_filter(sales_type), month)
             for month in range(1, 13)], rel=1e-9)
