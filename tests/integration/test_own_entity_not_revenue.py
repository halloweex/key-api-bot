"""A sale to the company's own legal entity is revenue nowhere, in Postgres
(the owner's decision of 2026-11-14).

Bronze is seeded as the landing writes it, and Postgres derives Silver and
Gold itself — the production path (`tests.integration.pg_tab_seed`). The
fixture's own-entity orders are on every sales type the entity has bought
on, and one on a retired source; beside them, an order with no buyer, and
ordinary orders of every kind, so that each figure below is also a figure
something else still counts. What is proved:

* **Silver keeps every order** and derives `is_active_source` FALSE for the
  entity's, TRUE for an order with no buyer — never NULL, or the insert would
  have failed and nothing below would have run;
* **Gold leaves them out of every measure, in both grains**: revenue,
  orders, the three customer counts, returns, the per-source rows and the
  roll-up, and the derivation's own validation still passes;
* **the readers that read around the column** leave them out against a real
  store: the goals' growth history, the /expenses profit, the subcategory
  drill-down and the assistant's top products;
* **the two integrity findings**: the INFO counting what is kept out, and
  the WARN for a card that reads like the entity and is on neither list,
  with the name matched by Postgres' `translate()` and `~` exactly as the
  Python mirror matches it.

The cohorts read ClickHouse; their membership is proved on DuckDB in memory
by `tests/unit/test_own_entity_not_revenue.py`. Skipped without `KS_PG_DSN`;
CI supplies one.
"""
from __future__ import annotations

import os
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

from core.duckdb_constants import OWN_ENTITY_BUYER_IDS
from tests.integration.pg_tab_seed import (
    derive, seed, store_that_never_reaches_duckdb, tab_in_postgres,
)

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

KYIV = ZoneInfo("Europe/Kyiv")
OWN = OWN_ENTITY_BUYER_IDS[0]
# Yesterday in Kyiv, at noon: the assistant's "yesterday" is the one window
# it can be asked for by name, and every other read here takes its dates.
DAY = datetime.now(KYIV).date() - timedelta(days=1)
AT = datetime.combine(DAY, time(12, 0), tzinfo=KYIV)
B2B, STAFF = 15, 99          # the wholesale manager; one on no retail list
LOOKALIKE, CHECKED = 990001, 990002

# (id, source_id, status_id, grand_total, buyer_id, manager_id)
ORDERS = [
    (9101, 1, 1, 1000.0, None, None),      # retail, no buyer: revenue
    (9102, 1, 1, 5000.0, OWN, B2B),        # the entity, b2b
    (9103, 1, 1, 0.0, OWN, B2B),           # the entity, b2b, a test shipment
    (9104, 2, 1, 7000.0, OWN, STAFF),      # the entity, internal
    (9105, 1, 19, 300.0, OWN, B2B),        # the entity, a return
    (9106, 1, 1, 2000.0, 5001, B2B),       # an ordinary b2b order
    (9107, 1, 1, 400.0, 5002, STAFF),      # an ordinary internal order
    (9108, 3, 1, 800.0, OWN, B2B),         # the entity on Opencart
    (9109, 1, 1, 1500.0, LOOKALIKE, B2B),  # a card like the entity's, unlisted
    (9110, 3, 1, 600.0, 5001, B2B),        # an ordinary order on Opencart
    (9111, 4, 1, 250.0, OWN, None),        # the entity, retail, on the site
    (9112, 1, 1, 700.0, CHECKED, B2B),     # a card like it, checked: not it
]
OWN_ORDERS = {o[0] for o in ORDERS if o[4] == OWN}
# (id, order_id, product_id, name, quantity, price_sold)
LINES = [
    (9101001, 9101, 1, "Cream", 2, 500.0),
    (9102001, 9102, 1, "Cream", 10, 500.0),
    (9103001, 9103, 1, "Cream", 2, 450.0),
    (9106001, 9106, 1, "Cream", 4, 500.0),
    (9109001, 9109, 1, "Cream", 3, 500.0),
    (9111001, 9111, 1, "Cream", 1, 250.0),
]
BUYERS = [
    (OWN, "ТОВ «КОРІАН СТОРІ»", None),
    (LOOKALIKE, "Korean-Story", None),
    (CHECKED, "Корiан Сторi", None),
    (5001, "Test Buyer", None),
    (5002, "Another Test Buyer", None),
]
# (id, order_id, amount): one on the entity's sale, one on an ordinary one.
EXPENSES = [(91, 9102, 300.0), (92, 9106, 100.0)]


def _counts_as_revenue(order) -> bool:
    """The rule, written out apart from the SQL that implements it."""
    oid, source, status, _total, buyer, _manager = order
    return source in (1, 2, 4, 5) and status != 19 and buyer != OWN


async def _populate(conn, pool):
    from core.landing_rows import EXPENSE_COLUMNS, expense_rows

    await seed(
        conn,
        categories=[(1, "Care", None), (2, "Creams", 1)],
        products=[(1, "Cream", 2, "BrandA", "SKU-1", 500.0)],
        orders=[(oid, src, st, total, AT, buyer, mgr, None)
                for oid, src, st, total, buyer, mgr in ORDERS],
        lines=LINES,
        buyers=BUYERS,
    )
    await derive(pool)
    rows = expense_rows([
        {"id": order, "expenses": [
            {"id": eid, "expense_type_id": 1, "amount": amount, "description": "x",
             "status": "paid", "payment_date": None, "created_at": AT}]}
        for eid, order, amount in EXPENSES])
    await conn.executemany(
        f"INSERT INTO bronze.expenses ({', '.join(EXPENSE_COLUMNS)}) "
        f"VALUES ({', '.join(f'${i}' for i in range(1, len(EXPENSE_COLUMNS) + 1))})",
        [tuple(r) for r in rows])
    await conn.execute("DELETE FROM meta.mirror_state WHERE table_name = 'bronze.expenses'")
    await conn.execute(
        "INSERT INTO meta.mirror_state (table_name, last_ok_at, backfilled_at)"
        " VALUES ('bronze.expenses', now(), now())")


@pytest_asyncio.fixture
async def own(tmp_path, monkeypatch):
    """`(store, pool)`: the fixture derived in Postgres, and the repository on
    a store whose DuckDB connection is a trap."""
    from core import duckdb_constants, pg_expenses_read

    monkeypatch.setattr(duckdb_constants, "NOT_OWN_ENTITY_BUYER_IDS", (CHECKED,))
    monkeypatch.setattr(pg_expenses_read, "_backfilled", False)
    monkeypatch.setattr(pg_expenses_read, "_checked_at", 0.0)
    monkeypatch.setattr(pg_expenses_read, "_check_failed", None)
    async with tab_in_postgres(DSN, monkeypatch, _populate) as pool:
        try:
            yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the own entity"), pool
        finally:
            async with pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM meta.mirror_state WHERE table_name = 'bronze.expenses'")


class TestSilver:
    @pytest.mark.asyncio
    async def test_every_order_stays_and_only_ours_leave_revenue(self, own):
        """Mutation: a bare `buyer_id NOT IN (...)` — NULL for 9101, and the
        insert of every Silver row fails; or the rule dropped — the entity's
        rows read TRUE."""
        _store, pool = own
        async with pool.acquire() as conn:
            rows = {r["id"]: r for r in await conn.fetch(
                "SELECT id, sales_type, is_active_source, buyer_id FROM silver.orders")}
        assert set(rows) == {o[0] for o in ORDERS}
        for order in ORDERS:
            assert rows[order[0]]["is_active_source"] is _counts_as_revenue(order), order
        assert rows[9101]["is_active_source"] is True and rows[9101]["buyer_id"] is None
        assert {rows[i]["sales_type"] for i in OWN_ORDERS} == {"b2b", "internal", "retail"}
        assert not any(rows[i]["is_active_source"] for i in OWN_ORDERS)

    @pytest.mark.asyncio
    async def test_the_entity_is_never_a_new_customer(self, own):
        _store, pool = own
        async with pool.acquire() as conn:
            assert await conn.fetchval(
                "SELECT count(*) FROM silver.orders WHERE buyer_id = $1 AND is_new_customer",
                OWN) == 0


class TestGold:
    @pytest.mark.asyncio
    async def test_no_measure_counts_the_entity_in_either_grain(self, own):
        """Every measure, by sales type and source, and the roll-up, equals
        the rule written out over the fixture. Mutation: any measure read
        around `is_active_source`."""
        _store, pool = own
        async with pool.acquire() as conn:
            gold = await conn.fetch(
                "SELECT sales_type, source_id, revenue, orders_count, unique_customers,"
                " new_customers, returning_customers, returns_count, returns_revenue"
                " FROM gold.daily_revenue WHERE date = $1", DAY)
            silver = {r["id"]: r["sales_type"] for r in await conn.fetch(
                "SELECT id, sales_type FROM silver.orders")}

        def expected(sales_type, source):
            kept = [o for o in ORDERS if silver[o[0]] == sales_type
                    and (source is None or o[1] == source) and _counts_as_revenue(o)]
            buyers = {o[4] for o in kept if o[4] is not None}
            return (sum(o[3] for o in kept), len(kept), len(buyers), len(buyers), 0, 0, 0.0)

        assert gold, "Gold was not derived"
        for row in gold:
            got = (float(row["revenue"]), row["orders_count"], row["unique_customers"],
                   row["new_customers"], row["returning_customers"], row["returns_count"],
                   float(row["returns_revenue"]))
            assert got == expected(row["sales_type"], row["source_id"]), dict(row)
        rollup = {r["sales_type"]: float(r["revenue"]) for r in gold if r["source_id"] is None}
        assert rollup == {"retail": 1000.0, "b2b": 4200.0, "internal": 400.0}

    @pytest.mark.asyncio
    async def test_the_derivation_still_validates(self, own):
        from core.pg_derivation import validate

        _store, pool = own
        async with pool.acquire() as conn:
            result = await validate(conn)
        assert result["passed"], result
        assert result["silver_revenue"] == result["gold_revenue"] == 5600.0


class TestTheReadersAroundTheColumn:
    @pytest.mark.asyncio
    async def test_the_goals_history(self, own):
        """It counts Opencart (OQ-1) and not the entity, on Opencart either."""
        from core.repositories.goals import _silver_history_where
        from core.sql_dialect import numbered

        _store, pool = own
        got = {}
        for sales_type in ("b2b", "internal", "retail", "all"):
            clause, params = _silver_history_where(sales_type)
            async with pool.acquire() as conn:
                got[sales_type] = {r[0] for r in await conn.fetch(
                    numbered(f"SELECT s.id FROM silver.orders s WHERE {clause}"), *params)}
        assert got == {
            "b2b": {9106, 9109, 9110, 9112},
            "internal": {9107},
            "retail": {9101},
            "all": {9101, 9106, 9107, 9109, 9110, 9112},
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sales_type, revenue, expenses", [
        ("b2b", 4800.0, 100.0), ("internal", 400.0, 0.0), ("all", 6200.0, 100.0)])
    async def test_the_profit(self, own, sales_type, revenue, expenses):
        """The entity's money and the cost on its sale both leave; Opencart's
        money stays, as it always has here."""
        store, _pool = own
        out = await store.get_profit_analysis(DAY, DAY, sales_type=sales_type)
        assert (out["metrics"]["totalRevenue"], out["metrics"]["totalExpenses"]) == (
            revenue, expenses)

    @pytest.mark.asyncio
    async def test_the_subcategory_drill_down(self, own):
        store, _pool = own
        out = await store.get_subcategory_breakdown(DAY, DAY, "Care", sales_type="b2b")
        assert (out["labels"], out["revenue"], out["quantity"]) == (["Creams"], [3500.0], [7])

    @pytest.mark.asyncio
    async def test_the_assistants_top_products(self, own):
        from core import chat_tools

        out = await chat_tools._get_top_products("yesterday", "revenue", 5)
        assert [(p["name"], p["quantity"], p["revenue"]) for p in out["products"]] == [
            ("Cream", 2, 1000.0)]


class TestTheFindings:
    @pytest.mark.asyncio
    async def test_the_info_and_the_warn(self, own):
        """The entity's orders that would be revenue but for the rule — not
        the return, not Opencart's — with their goods; and the one unlisted
        card, by id, not the listed one and not the checked one."""
        from core.data_quality import Severity
        from core.pg_warehouse_dq import OwnEntity, check_pg_warehouse, read_facts

        _store, pool = own
        facts = await read_facts(pool=pool)
        assert isinstance(facts.own_entity, OwnEntity), facts.own_entity
        assert sorted(facts.own_entity.orders) == [
            (9102, "b2b", 5000.0, 5000.0, False),
            (9103, "b2b", 0.0, 900.0, False),
            (9104, "internal", 7000.0, 0.0, False),
            (9111, "retail", 250.0, 250.0, False),
        ]
        assert facts.own_entity.unlisted == ((LOOKALIKE, 1, 1500.0),)
        found = {i.check_name: i for i in check_pg_warehouse(facts)}
        info, warn = found["pg_own_entity_orders"], found["pg_own_entity_unlisted"]
        assert (info.severity, info.count) == (Severity.INFO, 4)
        assert "12,250.00 billed, 6,150.00 of goods" in info.description
        assert (warn.severity, warn.count, warn.sample_ids) == (Severity.WARN, 1, (LOOKALIKE,))
        for issue in (info, warn):
            assert "Korean" not in issue.description and "КОРІАН" not in issue.description

    @pytest.mark.asyncio
    async def test_a_listed_card_files_no_warn(self, own, monkeypatch):
        """Listing the second card is the lever: the WARN clears, and the
        INFO counts it once Silver is rebuilt under the longer list."""
        from core import duckdb_constants
        from core.pg_warehouse_dq import check_pg_warehouse, read_facts

        _store, pool = own
        monkeypatch.setattr(duckdb_constants, "OWN_ENTITY_BUYER_IDS", (OWN, LOOKALIKE))
        before = {i.check_name: i for i in check_pg_warehouse(await read_facts(pool=pool))}
        assert "pg_own_entity_unlisted" not in before
        assert "1 more still count as revenue in stored Silver, worth 1,500.00" in \
            before["pg_own_entity_orders"].description
        await derive(pool)
        after = {i.check_name: i for i in check_pg_warehouse(await read_facts(pool=pool))}
        assert after["pg_own_entity_orders"].count == 5
        assert "still count" not in after["pg_own_entity_orders"].description


class TestTheNameRuleInPostgres:
    @pytest.mark.asyncio
    async def test_postgres_and_python_read_every_spelling_alike(self, own):
        """The fold by `translate()` and the pattern by `~`, over every
        spelling the unit test claims either way: the same verdict as the
        Python mirror. Mutation: fold with LOWER, which this database's
        production twin does not apply to Cyrillic, or a pattern construct
        the two engines read differently."""
        from core import pg_warehouse_dq as twins
        from tests.unit.test_own_entity_not_revenue import LOOKS_LIKE_THE_ENTITY, NOT_THE_ENTITY

        _store, pool = own
        async with pool.acquire() as conn:
            for name in LOOKS_LIKE_THE_ENTITY + NOT_THE_ENTITY:
                in_postgres = await conn.fetchval(
                    "SELECT translate($1, $2, $3) ~ $4", name,
                    twins.OWN_ENTITY_FOLD_UPPER + twins.OWN_ENTITY_FOLD_DROP,
                    twins.OWN_ENTITY_FOLD_LOWER, twins.OWN_ENTITY_NAME_PATTERN)
                assert in_postgres is twins.own_entity_name_matches(name), name
                assert in_postgres is (name in LOOKS_LIKE_THE_ENTITY), name


# ─── ClickHouse: the copy, its own Gold, the cohorts ────────────────────────
#
# ClickHouse never recomputes the rule: it receives Silver as Postgres stored
# it and derives its own Gold from `GOLD_MEASURES` over the copied column. So
# what has to hold is that the round trip carries the column, and that the
# two engines' Gold then agree cell for cell with the entity out of both.
# Skipped without `KS_CH_URL` (no ClickHouse in CI); point it at a throwaway
# 24.8 server, production's version.

needs_clickhouse = pytest.mark.skipif(
    not os.getenv("KS_CH_URL"), reason="needs a live ClickHouse at KS_CH_URL")


@needs_clickhouse
class TestClickHouse:
    @pytest.mark.asyncio
    async def test_the_copy_and_its_gold_agree_with_postgres(self, own):
        """`reconcile_clickhouse` ships this fixture's Silver, reads it back
        and sets ClickHouse's Gold against Postgres', both grains, at zero
        tolerance: no finding is agreement. Then ClickHouse's roll-up is read
        directly — the entity is out of it too, not merely equal to a wrong
        Postgres."""
        from core.ch_common import execute
        from core.ch_silver import GOLD_STATE, SILVER_STATE, reconcile_clickhouse

        _store, pool = own
        async with pool.acquire() as conn:
            saved = await conn.fetch(
                "SELECT * FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                [SILVER_STATE, GOLD_STATE])
        try:
            issues = await reconcile_clickhouse()
            assert [(i.check_name, i.description) for i in issues] == []
            day = DAY.isoformat()
            rollup = await execute(
                "SELECT sales_type, toFloat64(sum(revenue)), sum(orders_count)"
                " FROM gold.daily_revenue"
                f" WHERE source_id IS NULL AND date = toDate('{day}')"
                " GROUP BY sales_type ORDER BY sales_type FORMAT TabSeparated")
            assert [tuple(line.split("\t")) for line in rollup.strip().splitlines()] == [
                ("b2b", "4200", "3"), ("internal", "400", "1"), ("retail", "1000", "1")]
            flags = await execute(
                "SELECT id, is_active_source FROM silver.orders"
                f" WHERE buyer_id = {OWN} ORDER BY id FORMAT TabSeparated")
            assert [line.split("\t") for line in flags.strip().splitlines()] == [
                [str(oid), "false"] for oid in sorted(OWN_ORDERS)]
        finally:
            async with pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM meta.mirror_state WHERE table_name = ANY($1::text[])",
                    [SILVER_STATE, GOLD_STATE])
                for row in saved:
                    columns = list(row.keys())
                    await conn.execute(
                        f"INSERT INTO meta.mirror_state ({', '.join(columns)}) VALUES "
                        f"({', '.join(f'${i}' for i in range(1, len(columns) + 1))})",
                        *row.values())


@needs_clickhouse
@pytest.mark.asyncio
@pytest.mark.parametrize("name", [
    "cohort_retention_select", "enhanced_cohort_retention_select",
    "days_to_second_purchase_select", "cohort_ltv_select", "at_risk_customers_select"])
async def test_the_entity_is_in_no_cohort_on_clickhouse(name):
    """The O5 fixture on ClickHouse with one own-entity order beside it,
    `is_active_source` FALSE as Silver derives it: every body answers what it
    answers without the entity. Mutation: drop the membership clause — the
    entity joins March's cohort and every size there reads 4."""
    from tests.integration.test_cohorts_active_sources import _ask, _at_risk_today, _seed
    from tests.unit.test_cohort_active_sources import EXPECTED, ORDERS

    await _seed(ORDERS + [(301, OWN, "2026-03-15", 1, False, 9000)])
    rows = await _ask(name)
    assert rows == (_at_risk_today() if name == "at_risk_customers_select" else EXPECTED[name])
