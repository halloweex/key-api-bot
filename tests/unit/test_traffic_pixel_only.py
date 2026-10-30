"""Pixel-only orders on /traffic, by the buyer: new or returning, and the
channel their very first order came through.

`pixel_only` is a website order that arrived with no tags, only a pixel's
cookie. Measured on production 2026-10-28: of 1 616 such buyers over 180 days
38% had also bought through organic and 21% through ads, and in August 2026,
when the comment template lost its UTM, the bucket rose from ~30% of website
orders to 70%. These tests hold the rule that says who those people are:
"first" is Silver's own rule for a new buyer — every source, every sales
type, returns excluded, ties by id.

Since step 14's S14-05 the tab reads Postgres alone, so the repository's
Silver rows go into Postgres where they went into DuckDB when this was
written; those tests skip without `KS_PG_DSN`. The rows and the numbers are
the ones they were.

The filters the breakdown shares with the pixel-only card — the window's two
edges, the header's source and sales type, the sources that count — and what
the route hands the repository are held here too, with numbers written out
by hand. They were not until the review of S14-05's merge (2026-10-29):
eight mutations of them broke no test, one of them a default that would have
shown a viewer internal sales, which are admin-only.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Optional

import pytest
import pytest_asyncio

from core.utm_classify import FIRST_CHANNEL_OTHER, classify_traffic, first_order_channel
from tests.integration.pg_tab_seed import (
    needs_postgres, store_that_never_reaches_duckdb, tab_in_postgres,
)
from tests.unit.test_read_fallback_sites import admin_client  # noqa: F401 — fixture

DAY = date(2026, 8, 12)
WINDOW = (DAY - timedelta(days=1), DAY + timedelta(days=1))
PIXEL = {"_fbp": "fb.1.1.1"}
BIO = {"utm_source": "ig", "utm_medium": "social", "utm_content": "link_in_bio"}
FB_AD = {"utm_source": "fbads", "utm_medium": "paid", "utm_campaign": "spring"}


class TestTheRule:

    def test_each_kind_of_first_order(self):
        def first(utm=None, *, source_id=4):
            if utm is None:
                return first_order_channel(
                    source_id=source_id, tagged=False,
                    platform={1: "instagram", 2: "telegram"}.get(source_id, "unattributed"),
                    traffic_type="organic" if source_id in (1, 2) else "unknown",
                    utm_medium="", utm_campaign="")
            tt, platform = classify_traffic(utm)
            return first_order_channel(
                source_id=source_id, tagged=True, platform=platform,
                traffic_type=tt, utm_medium=utm.get("utm_medium"),
                utm_campaign=utm.get("utm_campaign"))

        assert first(source_id=1) == ("organic", "instagram_direct")
        assert first(BIO) == ("organic", "instagram_site")
        assert first(FB_AD) == ("paid", "facebook")
        assert first({"utm_source": "google", "utm_medium": "cpc"}) == ("paid", "google_ads")
        assert first(PIXEL) == ("pixel_only", "pixel_only")
        assert first({"utm_campaign": "sales_manager_anna"}) == ("manager", "manager")
        assert first(source_id=3) == ("unknown", "opencart")
        assert first(source_id=5) == ("unknown", "exhibition")
        assert first(source_id=4) == ("unknown", "unknown")

    def test_the_other_keys_are_the_ones_the_tab_names(self):
        assert set(FIRST_CHANNEL_OTHER) == {
            "pixel_only", "manager", "opencart", "exhibition", "unknown"}


# ─── The repository, on Postgres (DuckDB until step 14's S14-05) ─────────────

async def _nothing(conn, pool):
    """Each test writes the Silver rows it is about."""


@pytest_asyncio.fixture
async def tab(tmp_path, monkeypatch):
    """`(store, pool)`: the tab's store, which can only answer from Postgres,
    and the pool the rows go in through."""
    async with tab_in_postgres(os.environ["KS_PG_DSN"], monkeypatch, _nothing) as pool:
        yield store_that_never_reaches_duckdb(tmp_path, monkeypatch, "/traffic"), pool


async def _order(pool, order_id: int, buyer_id: Optional[int], *,
                 day: date = DAY, total: float = 100.0, source_id: int = 4,
                 utm: Optional[dict] = None, new: bool = False,
                 is_return: bool = False, active: bool = True,
                 sales_type: str = "retail",
                 manager_id: Optional[int] = None) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO silver.orders (
                id, source_id, status_id, grand_total, ordered_at, buyer_id,
                manager_id, order_date, is_return, sales_type, is_active_source,
                source_name, is_new_customer, buyer_first_order_date, promocode
            ) VALUES ($1, $2, 12, $3, NULL, $4, $9, $5, $6, $10, $7, 'x', $8, NULL, NULL)
            """,
            order_id, source_id, total, buyer_id, day, is_return, active, new,
            manager_id, sales_type,
        )
        if utm is not None:
            tt, platform = classify_traffic(utm)
            await conn.execute(
                "INSERT INTO silver.order_utm (order_id, utm_source, utm_medium,"
                " utm_campaign, utm_content, fbp, traffic_type, platform, parsed_at)"
                " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())",
                order_id, utm.get("utm_source"), utm.get("utm_medium"),
                utm.get("utm_campaign"), utm.get("utm_content"),
                utm.get("_fbp"), tt, platform,
            )


def _firsts(out: dict) -> dict:
    return {(r["group"], r["channel"]): r for r in out["first_channels"]}


@needs_postgres
class TestTheBreakdown:

    @pytest.mark.asyncio
    async def test_returning_buyers_are_placed_by_their_first_order(self, tab):
        """Buyer 1 first ordered in Instagram Direct, buyer 2 through the
        profile link, buyer 3 through a Facebook ad; each comes back to the
        website with no tags. Buyer 4's first order is this pixel order."""
        store, pool = tab
        before = DAY - timedelta(days=60)
        await _order(pool, 11, 1, day=before, source_id=1)
        await _order(pool, 21, 2, day=before, utm=BIO)
        await _order(pool, 31, 3, day=before, utm=FB_AD)
        await _order(pool, 12, 1, total=300, utm=PIXEL)
        await _order(pool, 22, 2, total=200, utm=PIXEL)
        await _order(pool, 32, 3, total=150, utm=PIXEL)
        await _order(pool, 41, 4, total=120, utm=PIXEL, new=True)
        out = await store.get_traffic_pixel_only(*WINDOW, sales_type="retail")

        firsts = _firsts(out)
        assert firsts[("organic", "instagram_direct")]["revenue"] == 300.0
        assert firsts[("organic", "instagram_site")]["buyers"] == 1
        assert firsts[("paid", "facebook")]["orders"] == 1
        assert firsts[("pixel_only", "pixel_only")]["new_buyers"] == 1
        assert out["new"] == {"orders": 1, "revenue": 120.0, "buyers": 1}
        assert out["returning"] == {"orders": 3, "revenue": 650.0, "buyers": 3}
        assert out["totals"] == {"orders": 4, "revenue": 770.0, "buyers": 4}
        assert [r["channel"] for r in out["first_channels"]][0] == "instagram_direct"

    @pytest.mark.asyncio
    async def test_first_counts_every_source_and_skips_returns(self, tab):
        """Silver's rule: buyer 5's first order was on the 2024 website
        (an inactive source), which still makes it the first; buyer 6's
        earliest order was cancelled, so the next one is the first."""
        store, pool = tab
        await _order(pool, 51, 5, day=DAY - timedelta(days=400),
                     source_id=3, active=False)
        await _order(pool, 52, 5, utm=PIXEL)
        await _order(pool, 61, 6, day=DAY - timedelta(days=90),
                     utm=FB_AD, is_return=True)
        await _order(pool, 62, 6, day=DAY - timedelta(days=30), utm=BIO)
        await _order(pool, 63, 6, utm=PIXEL)
        out = await store.get_traffic_pixel_only(*WINDOW, sales_type="retail")

        assert set(_firsts(out)) == {("unknown", "opencart"), ("organic", "instagram_site")}

    @pytest.mark.asyncio
    async def test_the_total_is_the_pixel_only_card(self, tab):
        store, pool = tab
        await _order(pool, 1, 1, total=300, utm=PIXEL, new=True)
        await _order(pool, 2, 2, total=200, utm=PIXEL)
        await _order(pool, 3, 3, total=999, utm=FB_AD)
        await _order(pool, 4, None, total=50, utm=PIXEL)
        out = await store.get_traffic_pixel_only(*WINDOW, sales_type="retail")
        card = (await store.get_traffic_analytics(
            *WINDOW, sales_type="retail"))["summary"]["pixel_only"]

        assert {k: out["totals"][k] for k in ("orders", "revenue")} == card
        assert ("unknown", "unknown") in _firsts(out)

    @pytest.mark.asyncio
    async def test_an_empty_window(self, tab):
        store, _pool = tab
        out = await store.get_traffic_pixel_only(*WINDOW, sales_type="retail")

        assert out["first_channels"] == []
        assert out["totals"] == {"orders": 0, "revenue": 0.0, "buyers": 0}


def _counts(out: dict) -> dict:
    return out["totals"]


@needs_postgres
class TestTheFilters:
    """The four predicates the breakdown shares with the pixel-only card:
    the window with both of its edges, the header's source and sales type,
    and the sources that count. Every pixel order above is a retail order
    on the website, on the window's middle day, so none of these could
    fail there. The numbers are written out by hand."""

    @pytest.mark.asyncio
    async def test_the_window_holds_both_of_its_edges_and_nothing_past_them(self, tab):
        store, pool = tab
        start, end = WINDOW
        await _order(pool, 1, 1, day=start, total=100, utm=PIXEL)
        await _order(pool, 2, 2, day=end, total=200, utm=PIXEL)
        await _order(pool, 3, 3, day=start - timedelta(days=1), total=400, utm=PIXEL)
        await _order(pool, 4, 4, day=end + timedelta(days=1), total=800, utm=PIXEL)
        out = await store.get_traffic_pixel_only(start, end, sales_type="retail")

        assert _counts(out) == {"orders": 2, "revenue": 300.0, "buyers": 2}

    @pytest.mark.asyncio
    async def test_a_source_narrows_the_orders_and_never_the_first_order(self, tab):
        """Buyer 2's first order was in Instagram Direct (source 1, no tag):
        asked for the website alone, their pixel order is still placed by
        that first order, which the source filter does not reach."""
        store, pool = tab
        await _order(pool, 1, 1, total=100, source_id=1, utm=PIXEL)
        await _order(pool, 21, 2, day=DAY - timedelta(days=60), source_id=1)
        await _order(pool, 22, 2, total=200, source_id=4, utm=PIXEL)

        instagram = await store.get_traffic_pixel_only(
            *WINDOW, sales_type="retail", source_id=1)
        website = await store.get_traffic_pixel_only(
            *WINDOW, sales_type="retail", source_id=4)
        every = await store.get_traffic_pixel_only(*WINDOW, sales_type="retail")

        assert _counts(instagram) == {"orders": 1, "revenue": 100.0, "buyers": 1}
        assert _counts(website) == {"orders": 1, "revenue": 200.0, "buyers": 1}
        assert set(_firsts(website)) == {("organic", "instagram_direct")}
        assert _counts(every) == {"orders": 2, "revenue": 300.0, "buyers": 2}

    @pytest.mark.asyncio
    async def test_a_sales_type_narrows_the_orders_and_all_does_not(self, tab):
        """Retail, the wholesale manager and an internal one, each with a
        pixel order. Buyer 1's first order was an internal one through a
        Facebook ad: "first" spans every sales type, so asked for retail
        alone that order still places them."""
        from core.duckdb_constants import B2B_MANAGER_ID

        store, pool = tab
        await _order(pool, 11, 1, day=DAY - timedelta(days=60), utm=FB_AD,
                     sales_type="internal", manager_id=99)
        await _order(pool, 12, 1, total=100, utm=PIXEL)
        await _order(pool, 2, 2, total=200, utm=PIXEL,
                     sales_type="b2b", manager_id=B2B_MANAGER_ID)
        await _order(pool, 3, 3, total=400, utm=PIXEL,
                     sales_type="internal", manager_id=99)

        def ask(sales_type=None):
            if sales_type is None:
                return store.get_traffic_pixel_only(*WINDOW)
            return store.get_traffic_pixel_only(*WINDOW, sales_type=sales_type)

        retail = await ask("retail")
        assert _counts(retail) == {"orders": 1, "revenue": 100.0, "buyers": 1}
        assert set(_firsts(retail)) == {("paid", "facebook")}
        assert _counts(await ask("b2b")) == {"orders": 1, "revenue": 200.0, "buyers": 1}
        assert _counts(await ask("internal")) == {"orders": 1, "revenue": 400.0, "buyers": 1}
        every = {"orders": 3, "revenue": 700.0, "buyers": 3}
        assert _counts(await ask("all")) == every
        assert _counts(await ask()) == every, "the repository's default is every sales type"

    @pytest.mark.asyncio
    async def test_a_retired_source_and_a_return_are_not_counted(self, tab):
        """An Opencart order (source 3, inactive) and a returned website
        order whose UTM rows both say pixel_only: neither is a pixel-only
        order of the period, here or on the card."""
        store, pool = tab
        await _order(pool, 1, 1, total=100, source_id=3, active=False, utm=PIXEL)
        await _order(pool, 2, 2, total=200, is_return=True, utm=PIXEL)
        await _order(pool, 3, 3, total=400, utm=PIXEL)
        out = await store.get_traffic_pixel_only(*WINDOW, sales_type="all")
        card = (await store.get_traffic_analytics(
            *WINDOW, sales_type="all"))["summary"]["pixel_only"]

        assert _counts(out) == {"orders": 1, "revenue": 400.0, "buyers": 1}
        assert card == {"orders": 1, "revenue": 400.0}


# ─── The route ───────────────────────────────────────────────────────────────

ROUTE = "/api/traffic/pixel-only"
DATES = {"start_date": WINDOW[0].isoformat(), "end_date": WINDOW[1].isoformat()}


class _Recorder:
    """A store that records what the route asked of it."""

    def __init__(self):
        self.calls: list = []

    async def get_traffic_pixel_only(self, **kwargs):
        self.calls.append(kwargs)
        return {"recorded": len(self.calls)}


@pytest.fixture
def recorder(monkeypatch):
    from unittest.mock import AsyncMock

    rec = _Recorder()
    monkeypatch.setattr("web.routes.api.traffic.get_store",
                        AsyncMock(return_value=rec))
    return rec


class TestTheRoute:
    """What `/api/traffic/pixel-only` hands the repository. The header's
    sales type defaults to retail — `internal` is admin-only, and a default
    of `all` would show it to every viewer — and every value is validated
    before anything is asked."""

    def test_with_no_sales_type_it_asks_for_retail(self, admin_client, recorder):
        r = admin_client.get(ROUTE, params=DATES)

        assert r.status_code == 200, r.text
        assert recorder.calls == [{"start_date": WINDOW[0], "end_date": WINDOW[1],
                                   "sales_type": "retail", "source_id": None}]

    @pytest.mark.parametrize("given, asked", [
        ("retail", "retail"), ("b2b", "b2b"), ("all", "all"), (" B2B ", "b2b"),
    ], ids=["retail", "b2b", "all", "folded"])
    def test_the_sales_type_reaches_the_store_validated(
            self, admin_client, recorder, given, asked):
        r = admin_client.get(ROUTE, params={**DATES, "sales_type": given})

        assert r.status_code == 200, r.text
        assert [c["sales_type"] for c in recorder.calls] == [asked]

    def test_the_source_reaches_the_store(self, admin_client, recorder):
        r = admin_client.get(ROUTE, params={**DATES, "source_id": 1,
                                            "sales_type": "b2b"})

        assert r.status_code == 200, r.text
        assert [(c["source_id"], c["sales_type"]) for c in recorder.calls] == [(1, "b2b")]

    @pytest.mark.parametrize("params", [
        {"sales_type": "junk"}, {"source_id": 99}, {"period": "fortnight"},
    ], ids=["sales_type", "source_id", "period"])
    def test_a_value_it_does_not_know_is_a_400_and_asks_nothing(
            self, admin_client, recorder, params):
        r = admin_client.get(ROUTE, params={**DATES, **params})

        assert r.status_code == 400, r.text
        assert recorder.calls == []


@needs_postgres
class TestTheRouteAnswers:
    """The same over HTTP against Postgres: what a viewer of the tab is
    answered with no sales type in the request is the retail answer."""

    @pytest.fixture
    def per_statement(self, monkeypatch):
        """The client runs each request on a loop of its own, which the
        test's pool does not belong to: one connection per statement."""
        import asyncpg

        from core.sql_dialect import numbered

        async def fetch(sql, params=()):
            conn = await asyncpg.connect(os.environ["KS_PG_DSN"])
            try:
                rows = await conn.fetch(numbered(sql), *params)
            finally:
                await conn.close()
            return [tuple(r) for r in rows]

        monkeypatch.setattr("core.pg_traffic_read.fetch", fetch)

    @pytest.mark.asyncio
    async def test_no_sales_type_is_the_retail_answer(
            self, tab, admin_client, per_statement, monkeypatch):
        import asyncio
        from unittest.mock import AsyncMock

        from core.duckdb_constants import B2B_MANAGER_ID

        store, pool = tab
        monkeypatch.setattr("web.routes.api.traffic.get_store",
                            AsyncMock(return_value=store))
        await _order(pool, 1, 1, total=100, utm=PIXEL)
        await _order(pool, 2, 2, total=200, utm=PIXEL,
                     sales_type="b2b", manager_id=B2B_MANAGER_ID)
        await _order(pool, 3, 3, total=400, source_id=1, utm=PIXEL)

        async def get(**params):
            r = await asyncio.to_thread(
                admin_client.get, ROUTE, params={**DATES, **params})
            return r.status_code, r.json()

        status, default = await get()
        assert status == 200, default
        assert default["totals"] == {"orders": 2, "revenue": 500.0, "buyers": 2}
        assert await get(sales_type="retail") == (200, default)
        assert (await get(sales_type="all"))[1]["totals"] == {
            "orders": 3, "revenue": 700.0, "buyers": 3}
        assert (await get(source_id=1))[1]["totals"] == {
            "orders": 1, "revenue": 400.0, "buyers": 1}
        assert (await get(sales_type="junk"))[0] == 400
