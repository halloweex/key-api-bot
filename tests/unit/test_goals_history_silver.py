"""The goal calculators' history is Silver's (chain 7b), and nothing else.

From DN-12 the calculators read DuckDB `orders` through a bridge that renders
`silver_sales_type_case` over DuckDB's `managers` and
`manager_classifications`; step 13 freezes all three. Chain 7b-2 read the same
history from `{silver_orders}` through the goal router under
`KS_GOALS_HISTORY=silver`, and production has read it since 2026-10-07 08:20
UTC. Chain 7b-4 deleted the bridge and retired the variable; step 14's S14-00
stopped it deciding anything, and S14-07 stopped anything reading it.

ONE ENGINE SINCE STEP 14 (S14-07). Every goal read is Postgres'
(`GoalsMixin._goals_run`), so what is proved here runs against Postgres —
Bronze seeded, Silver derived from it by Postgres itself, the production path
(`tests.integration.pg_tab_seed`) — and asks a store whose DuckDB connection
is a trap. Skipped without `KS_PG_DSN`, which CI supplies. The literal half
is `tests/integration/test_goals_history_two_engines.py`.

  * **the orders Silver selects** (T9) — on every branch of the sales-type
    CASE, for every sales type, the order ids written out here: KeyCRM's
    status group decides a return before the status list, and a source-3
    order — the 2024 website — is counted;
  * **Silver is what is read** (T10) — emptied, Silver answers nothing while
    Bronze still holds every order: no calculator reads `bronze.orders`, the
    table the deleted bridge read in DuckDB;
  * **the window and the source** (T12) — with Python's clock frozen inside
    the history, the YoY's full years and the last three complete months end
    at that Kyiv day, not the server's; and retail orders on Opencart move
    every baseline by what they hold, so no body filters `is_active_source`;
  * **the retired switch** (T13) — nothing in the code reads
    `KS_GOALS_HISTORY`, any value imports and answers alike, and its
    condition `goals_history_mode_invalid` stays retired (step 14's rule 15).

Each guard names the mutation that bites it.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
import pytest_asyncio

from core.duckdb_constants import KNOWN_SALES_TYPES
from core.models import OrderStatus
from core.repositories import goals as goals_module
from tests.integration.pg_tab_seed import (
    derive, needs_postgres, seed, store_that_never_reaches_duckdb, tab_in_postgres,
)
from tests.helpers.goals_history import (
    ALL_IDS, DSN, EXPECTED, INTERVALS, KYIV, MANAGERS, ORDERS, _calculators,
    _history_rows, _seed_history_pg,
)


# ─── T9: the orders Silver selects ─────────────────────────────────────────

SOURCE_3_RETAIL = 101    # the 2024 website on Opencart: no manager, retail
GROUP_6_UNLISTED = 102   # lost/cancel by KeyCRM's group, status not on the list
LISTED_NOT_GROUP_6 = 103  # on the status list, KeyCRM's group says it is not lost
LISTED_RETURN = 11       # the equivalence fixture's return: status 19, no group

# (id, source_id, status_id, status_group_id, manager_id, ordered_at)
EXTRA_ORDERS = [
    (SOURCE_3_RETAIL, 3, 1, None, None, ORDERS[0][4]),
    (GROUP_6_UNLISTED, 1, 1, 6, None, ORDERS[0][4]),
    (LISTED_NOT_GROUP_6, 1, 19, 4, None, ORDERS[0][4]),
]
assert 19 in {int(s) for s in OrderStatus.return_statuses()}
assert 1 not in {int(s) for s in OrderStatus.return_statuses()}
assert ORDERS[LISTED_RETURN - 1][:3] == (LISTED_RETURN, 4, 19)


def _expected_silver(scenario: str, sales_type: str) -> set:
    """What the history counts, written out: the fixture's sets for the
    sales type, less its one listed return, plus the three extra orders —
    each is retail (no manager), and of those the return rule keeps the
    one KeyCRM's group calls not lost and drops the one it calls lost."""
    base = ALL_IDS if sales_type == "all" else EXPECTED[scenario][sales_type]
    extra = {SOURCE_3_RETAIL, LISTED_NOT_GROUP_6} if sales_type in ("retail", "all") else set()
    return (set(base) - {LISTED_RETURN}) | extra


def _equivalence_and_more(scenario: str):
    """`populate` for `tab_in_postgres`: the equivalence fixture and the three
    extra orders in Bronze, the managers and their intervals as the scenario
    has them, and Silver derived by Postgres."""

    async def populate(conn, pool):
        if scenario != "nothing_synced":
            await conn.executemany(
                "INSERT INTO bronze.managers (id, name, is_retail) VALUES ($1, 'm', $2)",
                MANAGERS)
        if scenario == "classified":
            await conn.executemany(
                "INSERT INTO app.manager_classifications "
                "(manager_id, is_retail, valid_from, valid_to) VALUES ($1, $2, $3, $4)",
                INTERVALS)
        await seed(conn, orders=[
            (oid, src, st, 1000.0, when, 1, mgr, None) for oid, src, st, mgr, when in ORDERS])
        await conn.executemany(
            "INSERT INTO bronze.orders (id, source_id, status_id, status_group_id, "
            "grand_total, ordered_at, buyer_id, manager_id) "
            "VALUES ($1, $2, $3, $4, 1000.0, $5, 1, $6)",
            [(oid, src, st, grp, when, mgr) for oid, src, st, grp, mgr, when in EXTRA_ORDERS])
        await derive(pool)

    return populate


async def _silver_ids(pool, sales_type):
    from core.sql_dialect import numbered

    clause, params = goals_module._silver_history_where(sales_type)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            numbered(f"SELECT s.id FROM silver.orders s WHERE {clause}"), *params)
    return {r[0] for r in rows}


@pytest_asyncio.fixture
async def history_pg(tmp_path, monkeypatch):
    """The three-year history in Postgres and a store that never reaches
    DuckDB: `(store, pool)`."""
    async with tab_in_postgres(DSN, monkeypatch, _seed_history_pg) as pool:
        yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the goal history"), pool


class TestTheOrdersSilverSelects:
    """Written out, never compared with another reading — the bridge this
    was measured against until chain 7b-4 is gone. Mutations: M11 — add
    `is_active_source` to `_silver_history_where`, and the 2024 website's
    order is lost; M12 — decide returns by the status list instead of
    `is_return`, and 102 is counted while 103 is not; a sales-type rule
    changed in Silver's CASE moves an id between the written-out sets."""

    @needs_postgres
    @pytest.mark.asyncio
    @pytest.mark.parametrize("scenario", sorted(EXPECTED))
    async def test_every_sales_type_on_every_branch(self, monkeypatch, scenario):
        async with tab_in_postgres(DSN, monkeypatch, _equivalence_and_more(scenario)) as pool:
            async with pool.acquire() as conn:
                assert await conn.fetchval("SELECT COUNT(*) FROM silver.orders") \
                    == len(ORDERS) + len(EXTRA_ORDERS), "Silver was not built"
            for sales_type in (*KNOWN_SALES_TYPES, "all"):
                got = await _silver_ids(pool, sales_type)
                want = _expected_silver(scenario, sales_type)
                assert got == want, (scenario, sales_type, sorted(got ^ want))

    @needs_postgres
    @pytest.mark.asyncio
    async def test_the_partition_is_whole(self, monkeypatch):
        """Every order that is not a return lands in exactly one sales type,
        so no branch is empty by accident."""
        async with tab_in_postgres(DSN, monkeypatch, _equivalence_and_more("classified")) as pool:
            seen = [await _silver_ids(pool, t) for t in KNOWN_SALES_TYPES]
            everything = await _silver_ids(pool, "all")
        assert set().union(*seen) == everything
        assert sum(len(s) for s in seen) == len(everything)
        assert all(seen), "a sales type selected nothing — the fixture lost a branch"

    def test_the_where_carries_no_source_filter(self):
        for sales_type in (*KNOWN_SALES_TYPES, "all"):
            clause, _params = goals_module._silver_history_where(sales_type)
            assert "source" not in clause.lower(), clause
            assert "is_return" in clause

    def test_an_unknown_sales_type_refuses(self):
        with pytest.raises(ValueError, match="wholsale"):
            goals_module._silver_history_where("wholsale")


# ─── The bounds ───────────────────────────────────────────────────────────

class TestTheBounds:
    """`growth_metrics.period_start`/`period_end`, as the Monday job and the
    POST store them."""

    @needs_postgres
    @pytest.mark.asyncio
    async def test_the_bounds_read_every_order_every_status(self, tmp_path, monkeypatch):
        """`growth_metrics.period_start`/`period_end` are the first and last
        order of the whole history — a return, a b2b order and a source that
        is not a revenue source included. The history's own extremes are
        retail and not returns, so they cannot tell; these two outlie it.

        Mutation (review MXb2): `_SILVER_BOUNDS_SQL` narrowed to
        `WHERE NOT s.is_return AND s.sales_type = 'retail'` — the first bound
        becomes the history's own first day, and this fails.
        """
        from core.duckdb_constants import B2B_MANAGER_ID
        from tests.helpers.goals_history import HISTORY_END, HISTORY_START, _kyiv_noon

        first = HISTORY_START - timedelta(days=40)
        last = HISTORY_END + timedelta(days=40)

        async def populate(conn, pool):
            await seed(conn, orders=[
                # A retail return: status 19 is on the list.
                (990_001, 1, 19, 400.0, _kyiv_noon(first), 1, None, None),
                # A b2b order on Opencart, which is not a revenue source.
                (990_002, 3, 1, 600.0, _kyiv_noon(last), 1, B2B_MANAGER_ID, None),
            ])
            await _seed_history_pg(conn, pool)

        async with tab_in_postgres(DSN, monkeypatch, populate) as pool:
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT is_return, sales_type, is_active_source FROM silver.orders "
                    "WHERE id IN (990001, 990002) ORDER BY id")
            assert [tuple(r) for r in rows] == [
                (True, "retail", True), (False, "b2b", False)], rows

            store = store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the bounds")
            assert await store._history_bounds() == (first, last)
            tables = await store._compute_goal_tables(include_weekly=False)
            assert tables["history_bounds"] == (first, last)


class TestSilverIsWhatIsRead:
    """Not vacuous: the history really comes out of Silver. Emptied,
    Postgres' Silver answers nothing while Bronze still holds every order.
    Mutation: read any calculator from anything but `{silver_orders}` —
    `bronze.orders`, the deleted bridge's table — and it still answers here."""

    @needs_postgres
    @pytest.mark.asyncio
    async def test_an_emptied_silver_empties_the_answer(self, history_pg):
        store, pool = history_pg
        seasonality, yoy, _weekly, _caps = await _calculators(store, "retail")
        assert len(seasonality) == 12 and yoy["sample_size"] >= 1
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM silver.orders")
            assert await conn.fetchval("SELECT COUNT(*) FROM bronze.orders") > 1500
        seasonality, yoy, _weekly, caps = await _calculators(store, "retail")
        assert seasonality == {} and yoy["sample_size"] == 0
        assert caps == [0.35] * 12


# ─── T12: the window is Kyiv's, and the source is any ─────────────────────

# Half past midnight on New Year's Day in Kyiv, inside the history: the UTC
# clock still reads 2024-12-31, and the server's own today is past the
# history's end.
FROZEN = datetime(2025, 1, 1, 0, 30, tzinfo=KYIV)


class _FrozenClock(datetime):
    """`datetime` with `now()` at FROZEN, in whatever zone is asked."""

    @classmethod
    def now(cls, tz=None):
        return FROZEN.astimezone(tz) if tz else FROZEN.replace(tzinfo=None)


def _retail(year: int, month: int = 0) -> Decimal:
    """The history's retail revenue in a Kyiv year, or one of its months:
    its one retail order a day (no manager; the returns carry status 19)."""
    return sum((Decimal(str(total)) for _id, _src, status, total, when, manager
                in _history_rows()
                if manager is None and status == 1 and when.year == year
                and month in (0, when.month)), Decimal(0))


class TestTheWindowIsKyivs:
    """The two windows the history bounds by today — the YoY's full years,
    never the running one, and the last three complete months — end at
    Python's Kyiv day, here frozen at FROZEN. A bound read off the server's
    clock answers for today, a year and more past the history; one read in
    UTC, for 2024-12-31. Mutation M15: write `DATE_TRUNC('month',
    CURRENT_DATE)` back into the recent-months body."""

    @needs_postgres
    @pytest.mark.asyncio
    async def test_the_windows_end_at_the_kyiv_day(self, history_pg, monkeypatch):
        store, _pool = history_pg
        monkeypatch.setattr(goals_module, "datetime", _FrozenClock)

        recent = await store._recent_three_month_average("retail")
        assert recent == pytest.approx(
            float(sum(_retail(2024, month) for month in (10, 11, 12)) / 3))

        # 2025 is running and 2023 held one month: one full year, no pair.
        yoy = await store.calculate_yoy_growth("retail")
        assert [row["year"] for row in yoy["yearly_data"]] == [2024]
        assert yoy["yearly_data"][0]["revenue"] == pytest.approx(float(_retail(2024)))
        assert yoy["sample_size"] == 0


class TestTheWebsiteOnOpencartCounts:
    """OQ-1: the 2024 website sold on Opencart (source 3), which is not a
    revenue source, and a growth baseline counts it: retail orders on
    source 3 move every baseline by what they hold. Mutation M11: add
    `AND s.is_active_source` to any history body."""

    # One Opencart order a day through the first week of October 2024, so
    # the month's first week gains a share as well as money.
    DAYS = range(1, 8)
    EACH = Decimal("100.00")

    @needs_postgres
    @pytest.mark.asyncio
    async def test_a_retail_order_on_opencart_moves_every_baseline(
            self, history_pg, monkeypatch):
        from tests.helpers.goals_history import _kyiv_noon

        store, pool = history_pg
        monkeypatch.setattr(goals_module, "datetime", _FrozenClock)

        async def baselines():
            seasonality = await store.calculate_seasonality_indices("retail")
            yoy = await store.calculate_yoy_growth("retail")
            weekly = await store.calculate_weekly_patterns("retail")
            return {
                "october": seasonality[10]["avg_revenue"],
                "year_2024": yoy["yearly_data"][0]["revenue"],
                "a_year_before": await store._last_year_month_revenue(2025, 10, "retail"),
                "recent": await store._recent_three_month_average("retail"),
                "october_yoy": yoy["monthly_yoy"][10],
                "october_cap": await store._dynamic_growth_cap(10, "retail"),
                "october_week_1": weekly[10][1],
            }

        before = await baselines()
        async with pool.acquire() as conn:
            await seed(conn, orders=[
                (900_000 + day, 3, 1, float(self.EACH), _kyiv_noon(date(2024, 10, day)),
                 1, None, None)
                for day in self.DAYS])
        await derive(pool)
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT COUNT(*) FROM silver.orders WHERE source_id = 3 "
                "AND sales_type = 'retail' AND NOT is_active_source") == len(self.DAYS)
        after = await baselines()

        added = self.EACH * len(self.DAYS)
        moved = {key: after[key] - before[key] for key in
                 ("october", "year_2024", "a_year_before", "recent")}
        assert moved == pytest.approx({
            "october": float(added / 2),     # two Octobers in the history
            "year_2024": float(added),
            "a_year_before": float(added),
            "recent": float(added / 3),      # October to December 2024
        }), moved

        # One pair of Octobers: their growth, and the cap is that growth.
        october_2024 = _retail(2024, 10) + added
        growth = float((_retail(2025, 10) - october_2024) / october_2024)
        assert after["october_yoy"] == pytest.approx(growth, abs=5e-5)
        assert after["october_cap"] == pytest.approx(growth)
        assert after["october_week_1"] > before["october_week_1"]


# ─── T13: the retired switch ───────────────────────────────────────────────


class TestTheConditionStaysRetired:
    """`goals_history_mode_invalid` paged a refused `KS_GOALS_HISTORY` until
    S14-07. Its name is not reused (step 14's rule 15), and nothing emits it
    or publishes the block it judged. Mutation: register it again, give the
    canary a lever for it, or put `goals_history` back on `HealthResponse`."""

    NAME = "goals_history_mode_invalid"

    def test_it_is_not_a_condition_any_more(self):
        from core.alerting import REGISTRY

        assert self.NAME not in REGISTRY
