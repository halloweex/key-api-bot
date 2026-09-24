"""The goal calculators stop reading DuckDB Silver (DN-12, a bridge).

Seasonality, YoY, weekly patterns, the growth cap and the two history reads of
`generate_smart_goals` narrowed `orders` through an EXISTS over DuckDB
`silver_orders`. Silver stops moving at step 13, after which that EXISTS drops
every newer order; the next compaction empties it, the EXISTS matches nothing,
and `calculate_yoy_growth` wrote its 0.10 placeholder over the measured rate —
which the hourly full replace carried into Postgres.

The replacement renders the one definition, `silver_sales_type_case`, over
`orders o`. Four things are proved here:

  * **equivalence** — on every branch of that CASE, it selects exactly the order
    ids the Silver EXISTS selected while Silver was current, and the calculators
    built on it answer exactly what they answered before;
  * **compaction shape** — with `silver_orders` emptied, nothing moves;
  * **empty history** — the placeholder never overwrites a stored rate;
  * **the tripwire** — the bridge reads DuckDB `orders`, `managers` and
    `manager_classifications`, so it is right only while those chains have not
    moved. The test fails the moment one of them is registered as a write
    chain while `goals.py` still renders the DuckDB case.

The forecast signal's two-engine test lives with its siblings in
`tests/integration/test_goals_two_engines.py`.
"""
from __future__ import annotations

import ast
import asyncio
import logging
import statistics
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
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

REPO = Path(__file__).resolve().parents[2]
GOALS = REPO / "core" / "repositories" / "goals.py"
KYIV = ZoneInfo("Europe/Kyiv")
TIMEOUT_S = 60

# The old filter, verbatim, kept here as the reference the new predicate is
# measured against. It lived in `DuckDBStore._build_sales_type_filter`.
def _silver_exists(sales_type: str):
    if sales_type == "all":
        return "1=1", []
    return ("EXISTS (SELECT 1 FROM silver_orders sv "
            "WHERE sv.id = o.id AND sv.sales_type = ?)", [sales_type])


def _new(sales_type: str):
    from core.repositories.goals import _orders_sales_type_predicate
    return _orders_sales_type_predicate(sales_type)


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


def _ids(conn, predicate):
    clause, params = predicate
    return {r[0] for r in conn.execute(
        f"SELECT o.id FROM orders o WHERE {clause}", params).fetchall()}


@pytest_asyncio.fixture
async def store(tmp_path):
    s = DuckDBStore(db_path=tmp_path / "goals.duckdb")
    await s.connect()
    try:
        yield s
    finally:
        await s.close()


class TestTheNewPredicateSelectsWhatSilverSelected:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("scenario", sorted(EXPECTED))
    async def test_every_sales_type_on_every_branch(self, store, scenario):
        await _seed_equivalence(store, scenario)
        async with store.connection() as conn:
            silver_rows = conn.execute("SELECT COUNT(*) FROM silver_orders").fetchone()[0]
            assert silver_rows == len(ORDERS), "Silver was not built — nothing compared"
            for sales_type in (*KNOWN_SALES_TYPES, "all"):
                old = _ids(conn, _silver_exists(sales_type))
                new = _ids(conn, _new(sales_type))
                assert new == old, (scenario, sales_type, sorted(new ^ old))
                expected = (ALL_IDS if sales_type == "all"
                            else EXPECTED[scenario][sales_type])
                assert new == expected, (scenario, sales_type, sorted(new ^ expected))

    @pytest.mark.asyncio
    async def test_the_partition_is_whole(self, store):
        """Every order lands in exactly one sales_type, so no branch is empty
        by accident and the four sets above cannot overlap."""
        await _seed_equivalence(store, "classified")
        async with store.connection() as conn:
            seen = [_ids(conn, _new(t)) for t in KNOWN_SALES_TYPES]
        assert set().union(*seen) == ALL_IDS
        assert sum(len(s) for s in seen) == len(ALL_IDS)
        assert all(seen), "a sales type selected nothing — the fixture lost a branch"


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
# years and one pair of them. (Three full years would reach a separate, older
# defect — `yoy_rates` hold DuckDB DECIMALs and the recency weights are floats,
# so a second pair raises TypeError. Not this change's to fix; reported.)
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
    seasonality = await store.calculate_seasonality_indices(sales_type, persist=False)
    yoy = await store.calculate_yoy_growth(sales_type, persist=False)
    weekly = await store.calculate_weekly_patterns(sales_type, persist=False)
    async with store.connection() as conn:
        caps = [store._get_dynamic_growth_cap(conn, m, sales_type) for m in range(1, 13)]
    return _settled([seasonality, yoy, weekly, caps])


def _without_clock(smart):
    smart = dict(smart)
    smart["metadata"] = {k: v for k, v in smart["metadata"].items()
                         if k != "calculatedAt"}
    return _settled(smart)


async def _smart(store, sales_type):
    """Next month's smart goal, recomputing (and persisting) the three tables —
    the Monday job's path. Bounded: if the forecast read were taken inside the
    store lock again, this would hang rather than fail."""
    return _without_clock(await asyncio.wait_for(
        store.generate_smart_goals(2026, 10, sales_type, recalculate=True),
        TIMEOUT_S))


class TestTheCalculatorsAnswerWhatTheyAnsweredBefore:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["retail", "b2b"])
    async def test_through_the_new_predicate_and_through_the_silver_exists(
        self, store, sales_type,
    ):
        """OD-14: the bridge changes where the answer is read, never the answer.
        The same calculators, once through the old EXISTS and once through the
        new predicate, over a current Silver."""
        await _seed_history(store)
        new = await _calculators(store, sales_type)
        with patch("core.repositories.goals._orders_sales_type_predicate",
                   _silver_exists):
            old = await _calculators(store, sales_type)
        assert new == old
        seasonality, yoy, _weekly, _caps = new
        assert len(seasonality) == 12, "the fixture no longer reaches every month"
        assert yoy["sample_size"] >= 1, "no pair of full years — YoY compared nothing"


class TestCompactionShape:
    """`silver_orders` emptied — what a compaction leaves after step 13."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type", ["retail", "b2b", "all"])
    async def test_nothing_moves_when_silver_is_empty(self, store, sales_type):
        await _seed_history(store)
        before = await _calculators(store, sales_type)
        async with store.connection() as conn:
            conn.execute("DELETE FROM silver_orders")
        after = await _calculators(store, sales_type)
        assert after == before

        seasonality, yoy, weekly, _caps = after
        assert len(seasonality) == 12
        assert yoy["sample_size"] >= 1 and yoy["overall_yoy"] != 0.10
        assert all(len(weekly[m]) == 5 for m in range(1, 13))

    @pytest.mark.asyncio
    async def test_the_smart_goal_does_not_move_either(self, store):
        """`generate_smart_goals` read the same filter at two more sites — the
        growth cap and last year's month — and persists the three tables it
        recomputes. Gold is left alone: only Silver is emptied here."""
        await _seed_history(store)
        before = await _smart(store, "retail")
        async with store.connection() as conn:
            conn.execute("DELETE FROM silver_orders")
        after = await _smart(store, "retail")
        assert after == before
        assert before["monthly"]["lastYearRevenue"] > 0
        assert before["monthly"]["recent3MonthAvg"] > 0


class TestAnEmptyHistoryLeavesTheStoredRateAlone:
    STORED = 0.2345

    async def _stored_yoy(self, store):
        async with store.connection() as conn:
            row = conn.execute(
                "SELECT value, sample_size FROM growth_metrics "
                "WHERE metric_type = 'yoy_overall'").fetchone()
        return None if row is None else (float(row[0]), row[1])

    @pytest.mark.asyncio
    async def test_the_placeholder_does_not_overwrite_a_measured_rate(self, store):
        async with store.connection() as conn:
            conn.execute("DELETE FROM growth_metrics")
            conn.execute(
                "INSERT INTO growth_metrics (metric_type, value, sample_size) "
                "VALUES ('yoy_overall', ?, 3)", [self.STORED])

        result = await store.calculate_yoy_growth("retail")

        assert result["sample_size"] == 0 and result["overall_yoy"] == 0.10, (
            "the returned value is unchanged — only what is persisted is")
        assert await self._stored_yoy(store) == (self.STORED, 3)

    @pytest.mark.asyncio
    async def test_a_first_run_still_writes_the_placeholder(self, store):
        """Nothing better to keep: behaviour before DN-12, unchanged."""
        async with store.connection() as conn:
            conn.execute("DELETE FROM growth_metrics")
        await store.calculate_yoy_growth("retail")
        assert await self._stored_yoy(store) == (0.10, 0)

    @pytest.mark.asyncio
    async def test_a_real_history_still_overwrites(self, store):
        await _seed_history(store)
        async with store.connection() as conn:
            conn.execute(
                "INSERT INTO growth_metrics (metric_type, value, sample_size) "
                "VALUES ('yoy_overall', ?, 3)", [self.STORED])
        result = await store.calculate_yoy_growth("retail")
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


# ─── What the smart goal narrows to, in written-out numbers ─────────────────
#
# `TestTheCalculatorsAnswerWhatTheyAnsweredBefore` compares the old filter with
# the new one by patching the predicate, and `TestCompactionShape` compares a
# store with itself before and after — so a site that stops calling the
# predicate at all loses its filter on both sides and passes both (DN-12
# review, mutations x1, x2 and cap). These expect numbers computed here, in
# Python, from the rows as `_history_rows` wrote them.

RETURN_STATUSES = frozenset(int(s) for s in OrderStatus.return_statuses())


def _history_sales_type(source_id, manager_id):
    """The fixture's rows, typed by how they were written, not by the CASE."""
    if source_id == EXHIBITION_SOURCE_ID:
        return "exhibition"
    return "b2b" if manager_id == B2B_MANAGER_ID else "retail"


def _monthly_history(sales_type):
    """`{(year, month): (revenue, days with orders)}`, returns left out;
    `None` is every sales type — what a read that lost its filter sees."""
    revenue, days = defaultdict(Decimal), defaultdict(set)
    for _oid, src, status, total, when, mgr in _history_rows():
        if status in RETURN_STATUSES:
            continue
        if sales_type is not None and _history_sales_type(src, mgr) != sales_type:
            continue
        day = when.date()   # `when` is Kyiv-aware, so this is the Kyiv date
        revenue[(day.year, day.month)] += Decimal(str(total))
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
    async def test_last_year_the_recent_months_and_the_cap(self, store, sales_type):
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
        async with store.connection() as conn:
            caps = [store._get_dynamic_growth_cap(conn, m, "retail")
                    for m in range(1, 13)]
        assert caps == pytest.approx(
            [_expected_cap("retail", m) for m in range(1, 13)], rel=1e-9)


# ─── The tripwire ───────────────────────────────────────────────────────────
#
# The bridge reads three DuckDB tables. Once any of them changes hands, DuckDB's
# copy freezes, and retail goals silently diverge from Postgres Silver at the
# first reclassification (critique 8). Chain 5 (managers, classifications) and
# chain 3 (orders) must therefore wait for the chain 7b port — or for this
# predicate to be re-pointed at Postgres — and this is what enforces it.

PREDICATE_TABLES = frozenset(
    {"bronze.orders", "bronze.managers", "app.manager_classifications"})


def _renders_duckdb_case(source: str) -> list[int]:
    """Lines calling `silver_sales_type_case` for DuckDB — explicitly, by
    keyword, or through the default, which is DuckDB."""
    lines = []
    for node in ast.walk(ast.parse(source)):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
        if name != "silver_sales_type_case":
            continue
        args = list(node.args) + [k.value for k in node.keywords if k.arg == "dialect"]
        if not args or (isinstance(args[0], ast.Name) and args[0].id == "DUCKDB") \
                or (isinstance(args[0], ast.Constant) and args[0].value is None):
            lines.append(node.lineno)
    return lines


def _declared_chain_tables() -> dict[str, frozenset]:
    """`{module: CHAIN_TABLES}` for every chain — registered, and also any
    module under `core/` that declares the name, parsed rather than imported,
    so a chain that forgot to register cannot slip past."""
    from core.write_chains import WRITE_CHAINS, chain_name

    out = {chain_name(c): frozenset(c.CHAIN_TABLES) for c in WRITE_CHAINS}
    for path in (REPO / "core").rglob("*.py"):
        for node in ast.parse(path.read_text(encoding="utf-8")).body:
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                if any(isinstance(t, ast.Name) and t.id == "CHAIN_TABLES" for t in targets) \
                        and node.value is not None:
                    tables = {n.value for n in ast.walk(node.value)
                              if isinstance(n, ast.Constant) and isinstance(n.value, str)}
                    out.setdefault(path.stem, frozenset(tables))
    return out


def _tripped(chains: dict[str, frozenset], goals_source: str) -> dict[str, list[str]]:
    rendered = _renders_duckdb_case(goals_source)
    if not rendered:
        return {}
    return {name: sorted(tables & PREDICATE_TABLES)
            for name, tables in chains.items() if tables & PREDICATE_TABLES}


class TestTheTripwire:
    def test_no_chain_owns_what_the_bridge_reads(self):
        chains = _declared_chain_tables()
        assert chains, "no write chain found — the walk is not looking"
        tripped = _tripped(chains, GOALS.read_text(encoding="utf-8"))
        assert not tripped, (
            f"write chain(s) {tripped} own tables the goal calculators still "
            f"read from DuckDB (core/repositories/goals.py renders "
            f"silver_sales_type_case(DUCKDB) at lines "
            f"{_renders_duckdb_case(GOALS.read_text(encoding='utf-8'))}). Once "
            f"they move, DuckDB's copy freezes and retail goals diverge from "
            f"Postgres Silver at the first reclassification. Port chain 7b, or "
            f"re-point the predicate at Postgres, before moving these chains.")

    def test_the_bridge_is_what_it_watches_today(self):
        """Not vacuous: the detector sees today's rendering. When chain 7b
        lands and this fails, delete the tripwire with the bridge."""
        assert _renders_duckdb_case(GOALS.read_text(encoding="utf-8"))

    @pytest.mark.parametrize("table", sorted(PREDICATE_TABLES))
    def test_it_trips_on_each_table(self, table):
        source = "silver_sales_type_case(DUCKDB)"
        assert _tripped({"chain_x": frozenset({"app.other", table})}, source) \
            == {"chain_x": [table]}

    @pytest.mark.parametrize("call", [
        "silver_sales_type_case(DUCKDB)",
        "silver_sales_type_case()",
        "silver_sales_type_case(dialect=DUCKDB)",
        "store.silver_sales_type_case(None)",
    ])
    def test_it_sees_every_spelling_of_duckdb(self, call):
        assert _renders_duckdb_case(call) == [1]

    def test_a_postgres_rendering_does_not_trip(self):
        chains = {"chain_x": frozenset({"bronze.orders"})}
        assert _tripped(chains, "silver_sales_type_case(POSTGRES)") == {}
        assert _tripped(chains, "") == {}

    def test_a_chain_elsewhere_does_not_trip(self):
        chains = {"chain_x": frozenset({"app.manual_expenses", "bronze.offers"})}
        assert _tripped(chains, "silver_sales_type_case(DUCKDB)") == {}
