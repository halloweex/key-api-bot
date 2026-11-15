"""The offline store, in Postgres (the owner's decision of 2026-11-15:
retail, but offline, and a category of its own).

Bronze is seeded as the landing writes it, and Postgres derives Silver and
Gold itself — the production path (`tests.integration.pg_tab_seed`). The
fixture's store orders are sold by nobody, by the store's own seller (a
manager on no retail list, as production's is), by a manager on the retail
list, and one is cancelled, as every store order on production was on
2026-11-15; one is to the company's own legal entity. Beside them, retail and
internal orders, the store's seller's own sale on Instagram among them. What
is proved:

* **Silver** makes every store order `offline` whoever sold it, revenue
  except the own entity's, named `Офлайн-магазин`; the seller's Instagram
  sale is still `internal`;
* **Gold** carries the store in the roll-up and in source 7's fine rows, out
  of retail, and the derivation's validation — partition included — passes;
* **the API** answers `/api/summary` with real numbers for `sales_type=offline`
  and for `source_id=7`, over HTTP, and the retail answer is unmoved; the
  reports, the top products and the drill-down list the store;
* **the integrity layer** files no `pg_value_domain_orders_source_id`, which
  it does file for the same rows against the old list;
* **the reconciliation's Postgres arm** compares the store's orders;
* **/traffic** names the store's untagged sales `offline_store`, organic,
  and the organic block adds up to the organic card.

Skipped without `KS_PG_DSN`; CI supplies one.
"""
from __future__ import annotations

import asyncio
import os
from datetime import datetime, time, timedelta
from unittest.mock import AsyncMock, patch
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

from core.duckdb_constants import OFFLINE_STORE_SOURCE_ID, OWN_ENTITY_BUYER_IDS
from tests.integration.pg_tab_seed import (
    derive, seed, store_that_never_reaches_duckdb, tab_in_postgres,
)
from tests.unit.test_read_fallback_sites import admin_client  # noqa: F401 — fixture

asyncpg = pytest.importorskip("asyncpg")

DSN = os.getenv("KS_PG_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="needs a live PostgreSQL at KS_PG_DSN")

KYIV = ZoneInfo("Europe/Kyiv")
DAY = datetime.now(KYIV).date() - timedelta(days=1)
AT = datetime.combine(DAY, time(12, 0), tzinfo=KYIV)
STORE = OFFLINE_STORE_SOURCE_ID
OWN = OWN_ENTITY_BUYER_IDS[0]
SELLER, RETAIL_MANAGER = 44, 4   # the store's seller (on no list); one on RETAIL_MANAGER_IDS

# (id, source_id, status_id, grand_total, buyer_id, manager_id)
ORDERS = [
    (9201, STORE, 1, 800.0, 6001, SELLER),         # the seller's counter sale
    (9202, STORE, 1, 300.0, 6002, None),           # nobody named
    (9203, STORE, 1, 450.0, 6003, RETAIL_MANAGER),  # a retail manager at the counter
    (9204, STORE, 19, 500.0, 6004, SELLER),        # cancelled
    (9205, STORE, 1, 200.0, OWN, SELLER),          # to our own legal entity
    (9206, 1, 1, 1000.0, 6005, None),              # retail on Instagram
    (9207, 4, 1, 600.0, 6006, None),               # retail on the website
    (9208, 1, 1, 400.0, 6007, SELLER),             # the seller's own Instagram sale
]
STORE_ORDERS = {o[0] for o in ORDERS if o[1] == STORE}
# (id, order_id, product_id, name, quantity, price_sold)
LINES = [
    (9201001, 9201, 1, "Cream", 1, 800.0),
    (9203001, 9203, 1, "Cream", 1, 450.0),
    (9206001, 9206, 1, "Cream", 2, 500.0),
]


async def _populate(conn, pool):
    await seed(
        conn,
        categories=[(1, "Care", None), (2, "Creams", 1)],
        products=[(1, "Cream", 2, "BrandA", "SKU-1", 500.0)],
        orders=[(oid, src, st, total, AT, buyer, mgr, None)
                for oid, src, st, total, buyer, mgr in ORDERS],
        lines=LINES,
    )
    await derive(pool)


@pytest_asyncio.fixture
async def store_rows(tmp_path, monkeypatch):
    """`(store, pool)`: the fixture derived in Postgres, and the repository on
    a store whose DuckDB connection is a trap."""
    async with tab_in_postgres(DSN, monkeypatch, _populate) as pool:
        yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "the offline store"), pool


class TestSilver:
    @pytest.mark.asyncio
    async def test_every_store_order_is_offline_whoever_sold_it(self, store_rows):
        """Mutation: drop the CASE's branch — 9201 reads `internal`, 9202 and
        9203 `retail`; or leave 7 out of the revenue list — every store row
        reads FALSE."""
        _store, pool = store_rows
        async with pool.acquire() as conn:
            rows = {r["id"]: r for r in await conn.fetch(
                "SELECT id, sales_type, is_active_source, source_name FROM silver.orders")}
        assert {i: rows[i]["sales_type"] for i in STORE_ORDERS} == dict.fromkeys(
            STORE_ORDERS, "offline")
        assert {i: rows[i]["is_active_source"] for i in STORE_ORDERS} == {
            9201: True, 9202: True, 9203: True, 9204: True, 9205: False}
        assert {rows[i]["source_name"] for i in STORE_ORDERS} == {"Офлайн-магазин"}
        assert (rows[9206]["sales_type"], rows[9207]["sales_type"], rows[9208]["sales_type"]) == (
            "retail", "retail", "internal")


class TestGold:
    @pytest.mark.asyncio
    async def test_the_store_is_in_the_roll_up_and_its_own_source_and_not_in_retail(
            self, store_rows):
        _store, pool = store_rows
        async with pool.acquire() as conn:
            gold = {(r["sales_type"], r["source_id"]): r for r in await conn.fetch(
                "SELECT sales_type, source_id, revenue, orders_count, unique_customers,"
                " new_customers, returns_count, returns_revenue"
                " FROM gold.daily_revenue WHERE date = $1", DAY)}

        def measures(key):
            r = gold[key]
            return (float(r["revenue"]), r["orders_count"], r["unique_customers"],
                    r["new_customers"], r["returns_count"], float(r["returns_revenue"]))

        expected = (1550.0, 3, 3, 3, 1, 500.0)
        assert measures(("offline", None)) == expected
        assert measures(("offline", STORE)) == expected
        assert {k for k in gold if k[0] == "offline"} == {("offline", None), ("offline", STORE)}
        assert measures(("retail", None)) == (1600.0, 2, 2, 2, 0, 0.0)
        assert ("retail", STORE) not in gold

    @pytest.mark.asyncio
    async def test_the_derivation_still_validates_and_the_partition_is_whole(self, store_rows):
        """Mutation: leave `offline` out of KNOWN_SALES_TYPES — Gold's known
        types fall 1 550 short of Silver and the partition reads unknown."""
        from core.pg_derivation import validate

        _store, pool = store_rows
        async with pool.acquire() as conn:
            result = await validate(conn)
        assert result["passed"], result
        assert result["partition_exhaustive"] and result["unknown_sales_types"] == [], result
        assert result["silver_revenue"] == result["gold_revenue"] == 1550.0 + 1600.0 + 400.0


class TestTheDashboard:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("kwargs, expected", [
        ({"sales_type": "offline"}, (3, 1550.0, 1, 500.0)),
        ({"sales_type": "offline", "source_id": STORE}, (3, 1550.0, 1, 500.0)),
        ({"sales_type": "all", "source_id": STORE}, (3, 1550.0, 1, 500.0)),
        ({"sales_type": "retail", "source_id": STORE}, (0, 0.0, 0, 0.0)),
        ({"sales_type": "retail"}, (2, 1600.0, 0, 0.0)),
        ({"sales_type": "offline", "brand": "BrandA"}, (2, 1250.0, 1, 500.0)),
    ], ids=["offline", "offline-store", "all-store", "retail-store", "retail", "brand"])
    async def test_the_summary(self, store_rows, kwargs, expected):
        store, _pool = store_rows
        out = await store.get_summary_stats(DAY, DAY, **kwargs)
        assert (out["totalOrders"], out["totalRevenue"], out["totalReturns"],
                out["returnsRevenue"]) == expected

    @pytest.mark.asyncio
    async def test_the_trend_for_the_store_reads_its_gold_rows(self, store_rows):
        store, _pool = store_rows
        out = await store.get_revenue_trend(
            DAY, DAY, source_id=STORE, sales_type="all", include_comparison=False)
        assert out["revenue"] == [1550.0] and out["orders"] == [3]

    @pytest.mark.asyncio
    async def test_reports_top_products_and_the_drill_down_list_the_store(self, store_rows):
        store, _pool = store_rows
        report = await store.get_report_summary(DAY, DAY, sales_type="all")
        rows = {s["source_id"]: s for s in report["sources"]}
        assert (rows[STORE]["source_name"], rows[STORE]["orders_count"],
                rows[STORE]["revenue"], rows[STORE]["returns_count"]) == (
            "Офлайн-магазин", 3, 1550.0, 1)
        assert report["totals"]["revenue"] == 1550.0 + 1600.0 + 400.0
        by_source = await store.get_report_products_by_source(
            DAY, DAY, sales_type="offline", source_id=STORE)
        assert by_source == {STORE: [{"product_name": "Cream", "quantity": 2}]}
        top = await store.get_top_products(DAY, DAY, source_id=STORE, sales_type="offline")
        assert (top["labels"], top["data"]) == (["Cream"], [2])
        drill = await store.get_subcategory_breakdown(
            DAY, DAY, "Care", source_id=STORE, sales_type="offline")
        assert (drill["labels"], drill["revenue"], drill["quantity"]) == (
            ["Creams"], [1250.0], [2])


class _OneConnection:
    async def __aenter__(self):
        self._conn = await asyncpg.connect(DSN)
        return self._conn

    async def __aexit__(self, *exc):
        await self._conn.close()


class _PerStatementPool:
    """The client runs each request on a loop of its own, which the test's
    pool does not belong to: one connection per acquire."""

    def acquire(self):
        return _OneConnection()


class TestOverHttp:
    @pytest.mark.asyncio
    async def test_the_summary_route_answers_the_store(
            self, store_rows, admin_client, monkeypatch):
        """`/api/summary` itself: `source_id=7` and `sales_type=offline` pass
        the route's validation and the gate and are answered from Gold. Until
        the decision both were a 400. The exhibition is still one (decision 5)."""
        store, _pool = store_rows
        monkeypatch.setattr("web.services.dashboard_service.get_store",
                            AsyncMock(return_value=store))
        dates = {"start_date": DAY.isoformat(), "end_date": DAY.isoformat()}

        async def get(**params):
            r = await asyncio.to_thread(
                admin_client.get, "/api/summary", params={**dates, **params})
            return r.status_code, r.json()

        with patch("core.pg.get_pool", new=AsyncMock(return_value=_PerStatementPool())):
            for params in ({"sales_type": "offline"}, {"source_id": STORE, "sales_type": "all"}):
                status, body = await get(**params)
                assert status == 200, body
                assert (body["totalOrders"], body["totalRevenue"], body["totalReturns"]) == (
                    3, 1550.0, 1), params
            status, body = await get()
            assert (status, body["totalOrders"], body["totalRevenue"]) == (200, 2, 1600.0)
            assert (await get(source_id=5))[0] == 400


class TestTheIntegrityLayer:
    @pytest.mark.asyncio
    async def test_source_seven_is_known(self, store_rows, monkeypatch):
        """No `pg_value_domain_orders_source_id` over the store's rows — and,
        against the list as it was, the WARN production carried, so the check
        is shown to look."""
        from core import pg_warehouse_dq as twins

        _store, pool = store_rows

        async def landing():
            facts = await twins.read_facts(pool=pool)
            # Watched, or "no finding" would prove nothing.
            assert isinstance(facts.order_landing, twins.OrderLanding), facts.order_landing
            return {i.check_name: i for i in twins.check_pg_warehouse(facts)}

        # The landing facts are read once bronze.orders' history is backfilled.
        async with pool.acquire() as conn:
            saved = await conn.fetch(
                "SELECT * FROM meta.mirror_state WHERE table_name = 'bronze.orders'")
            await conn.execute(
                "INSERT INTO meta.mirror_state (table_name, last_attempted_at, last_ok_at,"
                " failures_since_ok, backfilled_at) VALUES ('bronze.orders', now(), now(), 0, now())"
                " ON CONFLICT (table_name) DO UPDATE SET backfilled_at = now()")
        try:
            assert "pg_value_domain_orders_source_id" not in await landing()

            monkeypatch.setattr(twins, "KNOWN_SOURCE_IDS", frozenset({1, 2, 3, 4, 5}))
            issue = (await landing())["pg_value_domain_orders_source_id"]
            assert issue.count == len(STORE_ORDERS)
            assert set(issue.sample_ids) <= STORE_ORDERS
            assert "Unknown values seen: 7" in issue.description
        finally:
            async with pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM meta.mirror_state WHERE table_name = 'bronze.orders'")
                for row in saved:
                    columns = list(row.keys())
                    await conn.execute(
                        f"INSERT INTO meta.mirror_state ({', '.join(columns)}) VALUES "
                        f"({', '.join(f'${i}' for i in range(1, len(columns) + 1))})",
                        *row.values())


class TestTheReconciliation:
    @pytest.mark.asyncio
    async def test_the_postgres_arm_compares_the_store(self, store_rows):
        from core.reconciliation_io import postgres_orders_in_window

        _store, pool = store_rows
        facts = await postgres_orders_in_window(
            pool, DAY, DAY, watermark=datetime.now(KYIV) + timedelta(hours=1))
        assert STORE_ORDERS <= set(facts)
        assert {facts[i]["source_id"] for i in STORE_ORDERS} == {STORE}


class TestTraffic:
    @pytest.mark.asyncio
    async def test_the_store_is_its_own_platform_and_organic(self, store_rows):
        """Mutation: drop the fallback's branch — the store's 1 550 reads as
        the website's `unattributed`, traffic type `unknown`."""
        store, _pool = store_rows
        out = await store.get_traffic_analytics(DAY, DAY, sales_type="offline")
        assert out["by_platform"] == {"offline_store": {"orders": 3, "revenue": 1550.0}}
        assert out["summary"]["organic"] == {"orders": 3, "revenue": 1550.0}
        retail = await store.get_traffic_analytics(DAY, DAY, sales_type="retail")
        assert "offline_store" not in retail["by_platform"]

    @pytest.mark.asyncio
    async def test_the_organic_block_names_it_and_adds_up_to_the_card(self, store_rows):
        store, _pool = store_rows
        card = (await store.get_traffic_analytics(DAY, DAY, sales_type="all"))["summary"]["organic"]
        block = await store.get_traffic_organic(DAY, DAY, sales_type="all")
        channels = {c["channel"]: c for c in block["channels"]}
        assert (channels["offline_store"]["orders"], channels["offline_store"]["revenue"]) == (
            3, 1550.0)
        assert (block["totals"]["orders"], block["totals"]["revenue"]) == (
            card["orders"], card["revenue"])

    @pytest.mark.asyncio
    async def test_the_transactions_and_campaigns_filter_by_it(self, store_rows):
        store, _pool = store_rows
        tx = await store.get_traffic_transactions(
            DAY, DAY, sales_type="all", platform="offline_store")
        assert tx["total"] == 3
        assert {t["id"] for t in tx["transactions"]} == {9201, 9202, 9203}
        assert {(t["platform"], t["traffic_type"], t["source"]) for t in tx["transactions"]} == {
            ("offline_store", "organic", "Офлайн-магазин")}
        campaigns = await store.get_traffic_utm_campaigns(
            DAY, DAY, sales_type="offline", traffic_type="organic")
        assert campaigns["total"] == 1
