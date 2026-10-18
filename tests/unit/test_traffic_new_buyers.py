"""New buyers on /traffic: people counted where their first order came from.

The number the tab did not have: of the buyers whose first order ever falls in
the window, how many came through each kind of traffic. It reads Silver's own
`is_new_customer`, the flag Gold's `new_customers` counts, so the tab and the
dashboard's cards cannot disagree about who is new — only about where they
came from, which is the tab's question.

What makes it more than a third column beside orders and revenue is that
people do not add up. `is_new_customer` marks every order on a buyer's first
day, and a buyer whose first day carried two orders through two channels is
new in both — and one person in the total. Orders fold; buyers do not.

The two-engine half is `tests/integration/test_traffic_two_engines.py`, whose
whole-payload comparison carries this block.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pytest

from core.duckdb_store import DuckDBStore

DAY = date(2026, 8, 12)
WINDOW = (DAY - timedelta(days=1), DAY + timedelta(days=1))
SUMMARY_KEYS = ("paid", "paid_confirmed", "paid_likely", "organic",
                "manager", "pixel_only", "unknown")


async def _store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "new_buyers.duckdb")
    await s.connect()
    return s


async def _order(store: DuckDBStore, order_id: int, buyer_id: int, *,
                 new: bool, source_id: int = 4,
                 verdict: Optional[tuple[str, str]] = None) -> None:
    """One Silver order, and its UTM verdict `(traffic_type, platform)` when
    the parser gave it one. No verdict is the LEFT JOIN's NULL: the source
    decides, as it does for most Instagram orders in production."""
    first = DAY if new else DAY - timedelta(days=90)
    async with store.connection() as conn:
        conn.execute(
            """
            INSERT INTO silver_orders (
                id, source_id, status_id, grand_total, ordered_at, buyer_id,
                manager_id, order_date, is_return, sales_type, is_active_source,
                source_name, is_new_customer, buyer_first_order_date, promocode
            ) VALUES (?, ?, 12, 100.0, NULL, ?, NULL, ?, FALSE, 'retail',
                      TRUE, 'x', ?, ?, NULL)
            """,
            [order_id, source_id, buyer_id, DAY, new, first],
        )
        if verdict is not None:
            conn.execute(
                "INSERT INTO silver_order_utm (order_id, traffic_type, platform,"
                " parsed_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                [order_id, *verdict],
            )


async def _new_customers(store: DuckDBStore) -> dict:
    out = await store.get_traffic_analytics(
        start_date=WINDOW[0], end_date=WINDOW[1], sales_type="retail")
    return out["new_customers"]


class TestANewBuyerIsCountedWhereTheyCameFrom:

    @pytest.mark.asyncio
    async def test_only_the_first_order_makes_a_buyer_new(self, tmp_path):
        """Buyer 10 arrived through a Facebook ad this week; buyer 11 has
        bought before and came back organically. One new buyer, and it is
        the paid one."""
        store = await _store(tmp_path)
        try:
            await _order(store, 1, 10, new=True,
                         verdict=("paid_confirmed", "facebook"))
            await _order(store, 2, 11, new=False, source_id=1)

            new = await _new_customers(store)
        finally:
            await store.close()

        assert new["total"] == 1
        assert new["summary"]["paid"] == 1
        assert new["summary"]["paid_confirmed"] == 1
        assert new["summary"]["organic"] == 0
        assert new["by_platform"] == {"facebook": 1}
        assert new["by_traffic_type"] == {"paid_confirmed": 1}

    @pytest.mark.asyncio
    async def test_an_order_with_no_verdict_falls_back_like_the_orders_do(
        self, tmp_path,
    ):
        """No UTM row at all: Instagram's source makes it organic/instagram,
        Shopify's makes it unknown/unattributed — the same COALESCE the
        order counts go through, or the two halves of one card would
        disagree about which card an order is on."""
        store = await _store(tmp_path)
        try:
            await _order(store, 1, 10, new=True, source_id=1)
            await _order(store, 2, 11, new=True, source_id=4)

            new = await _new_customers(store)
        finally:
            await store.close()

        assert new["summary"]["organic"] == 1
        assert new["summary"]["unknown"] == 1
        assert new["by_platform"] == {"instagram": 1, "unattributed": 1}

    @pytest.mark.asyncio
    async def test_google_is_split_the_way_the_platform_chart_splits_it(
        self, tmp_path,
    ):
        store = await _store(tmp_path)
        try:
            await _order(store, 1, 10, new=True,
                         verdict=("paid_confirmed", "google"))
            await _order(store, 2, 11, new=True, verdict=("organic", "google"))

            new = await _new_customers(store)
        finally:
            await store.close()

        assert new["by_platform"] == {"google_ads": 1, "google_organic": 1}


class TestPeopleDoNotAddUp:

    @pytest.mark.asyncio
    async def test_a_buyer_new_through_two_channels_is_one_person(self, tmp_path):
        """Buyer 20's first day: an ad order and an Instagram DM order. New
        on both cards — each card answers "how many new buyers came this
        way" — and one buyer in the total. Folding the per-group counts, as
        the orders are folded, would make the total two."""
        store = await _store(tmp_path)
        try:
            await _order(store, 1, 20, new=True,
                         verdict=("paid_confirmed", "facebook"))
            await _order(store, 2, 20, new=True, source_id=1)

            new = await _new_customers(store)
        finally:
            await store.close()

        assert new["summary"]["paid"] == 1
        assert new["summary"]["organic"] == 1
        assert new["by_platform"] == {"facebook": 1, "instagram": 1}
        assert new["total"] == 1

    @pytest.mark.asyncio
    async def test_paid_is_the_union_of_its_two_halves(self, tmp_path):
        """The paid card is confirmed + likely. For orders that is a sum; for
        a buyer with one of each on their first day it is still one buyer."""
        store = await _store(tmp_path)
        try:
            await _order(store, 1, 30, new=True,
                         verdict=("paid_confirmed", "facebook"))
            await _order(store, 2, 30, new=True,
                         verdict=("paid_likely", "tiktok"))

            new = await _new_customers(store)
        finally:
            await store.close()

        assert new["summary"]["paid_confirmed"] == 1
        assert new["summary"]["paid_likely"] == 1
        assert new["summary"]["paid"] == 1
        assert new["total"] == 1

    @pytest.mark.asyncio
    async def test_two_orders_of_one_buyer_through_one_channel_are_one(
        self, tmp_path,
    ):
        store = await _store(tmp_path)
        try:
            await _order(store, 1, 40, new=True,
                         verdict=("paid_confirmed", "facebook"))
            await _order(store, 2, 40, new=True,
                         verdict=("paid_confirmed", "facebook"))

            new = await _new_customers(store)
        finally:
            await store.close()

        assert new["by_platform"] == {"facebook": 1}
        assert new["summary"]["paid_confirmed"] == 1


class TestTheBlockIsAlwaysWhole:

    @pytest.mark.asyncio
    async def test_a_period_with_no_new_buyers_still_names_every_card(
        self, tmp_path,
    ):
        """The frontend reads all five cards unconditionally, and a returning
        buyer's order is the commonest thing there is."""
        store = await _store(tmp_path)
        try:
            await _order(store, 1, 10, new=False, source_id=1)

            new = await _new_customers(store)
        finally:
            await store.close()

        assert new == {
            "total": 0,
            "summary": {k: 0 for k in SUMMARY_KEYS},
            "by_platform": {},
            "by_traffic_type": {},
        }

    @pytest.mark.asyncio
    async def test_the_order_buckets_keep_their_shape(self, tmp_path):
        """The buyers ride beside the buckets, not inside them: the weekly
        traffic report and the reclassify dry run both read the buckets as
        `{orders, revenue}` and compare them whole."""
        store = await _store(tmp_path)
        try:
            await _order(store, 1, 10, new=True,
                         verdict=("paid_confirmed", "facebook"))
            out = await store.get_traffic_analytics(
                start_date=WINDOW[0], end_date=WINDOW[1], sales_type="retail")
        finally:
            await store.close()

        assert out["by_platform"]["facebook"] == {"orders": 1, "revenue": 100.0}
        assert set(out["summary"]["paid"]) == {"orders", "revenue"}
