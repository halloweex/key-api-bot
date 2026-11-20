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
is a trap. Skipped without `KS_PG_DSN`, which CI supplies; the walks of the
module and of the tree need no server. The literal half is
`tests/integration/test_goals_history_two_engines.py`.

  * **the orders Silver selects** (T9) — on every branch of the sales-type
    CASE, for every sales type, the order ids written out here: KeyCRM's
    status group decides a return before the status list, and a source-3
    order — the 2024 website — is counted;
  * **met by construction** (T10) — no statement any calculator, the smart
    goal or the Monday job sends Postgres names `bronze.orders`,
    `bronze.managers` or `app.manager_classifications`, the tables the
    deleted bridge read in DuckDB;
  * **the window** (T12) — no Silver body bounds a window with the server's
    clock, none filters `is_active_source`, none names a bridge table — read
    off the module, never listed;
  * **the retired switch** (T13) — nothing in the code reads
    `KS_GOALS_HISTORY`, any value imports and answers alike, and its
    condition `goals_history_mode_invalid` stays retired (step 14's rule 15).

Each guard names the mutation that bites it.
"""
from __future__ import annotations

import ast
import re
from datetime import timedelta
from pathlib import Path

import pytest
import pytest_asyncio

from core.duckdb_constants import KNOWN_SALES_TYPES
from core.models import OrderStatus
from core.repositories import goals as goals_module
from tests.integration.pg_tab_seed import (
    derive, needs_postgres, seed, store_that_never_reaches_duckdb, tab_in_postgres,
)
from tests.helpers.goals_history import (
    ALL_IDS, DSN, EXPECTED, INTERVALS, MANAGERS, ORDERS, _calculators,
    _seed_history_pg,
)

REPO = Path(__file__).resolve().parents[2]
GOALS = REPO / "core" / "repositories" / "goals.py"

# The three tables the deleted DN-12 bridge read out of DuckDB, as SQL names,
# and their Postgres homes: a goal read naming any of them is reading the
# orders or the classification around Silver.
BRIDGE_TABLES = ("orders", "managers", "manager_classifications")
_BRIDGE_TABLE = re.compile(
    r"(?<![\w.])(" + "|".join(BRIDGE_TABLES) + r")(?![\w])", re.IGNORECASE)


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


# ─── T12: the window and the row set, read off the module ─────────────────

def _silver_bodies() -> dict:
    """Every module-level statement in goals.py that reads `{silver_orders}`
    — parsed, so a body added tomorrow is held to these rules the day it is
    written."""
    tree = ast.parse(GOALS.read_text(encoding="utf-8"))
    return {node.targets[0].id: node.value.value
            for node in tree.body
            if isinstance(node, ast.Assign) and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
            and "{silver_orders}" in node.value.value}


SERVER_CLOCKS = re.compile(
    r"CURRENT_DATE|CURRENT_TIMESTAMP|LOCALTIMESTAMP|\bnow\s*\(|\btoday\s*\(",
    re.IGNORECASE)


class TestTheBodies:
    """Mutation M15: write `DATE_TRUNC('month', CURRENT_DATE)` back into the
    recent-months body — the engines disagree about that day for three hours
    out of twenty-four — and it fails here, named."""

    def test_the_walk_finds_the_history_bodies(self):
        found = set(_silver_bodies())
        history = {name for name in found if name.startswith("_SILVER_")}
        assert {"_SILVER_SEASONALITY_SQL", "_SILVER_YEARLY_SQL",
                "_SILVER_MONTHLY_YOY_SQL", "_SILVER_BOUNDS_SQL",
                "_SILVER_WEEKLY_SQL", "_SILVER_GROWTH_CAP_SQL",
                "_SILVER_LAST_YEAR_SQL", "_SILVER_RECENT_MONTHS_SQL"} <= history

    @pytest.mark.parametrize("name", sorted(_silver_bodies()))
    def test_no_body_reads_the_servers_clock(self, name):
        body = _silver_bodies()[name]
        # `TODAY_IN_KYIV` is spliced in at the read, never written here.
        assert not SERVER_CLOCKS.search(body), (name, SERVER_CLOCKS.search(body))

    @pytest.mark.parametrize("name", sorted(
        n for n in _silver_bodies() if n.startswith("_SILVER_")))
    def test_no_history_body_filters_the_source_or_names_the_bridge(self, name):
        body = _silver_bodies()[name]
        assert "is_active_source" not in body, (
            f"{name} drops the 2024 website from the growth baseline (OQ-1)")
        assert not _BRIDGE_TABLE.search(body), name


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
