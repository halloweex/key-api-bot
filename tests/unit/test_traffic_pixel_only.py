"""Pixel-only orders on /traffic, by the buyer: new or returning, and the
channel their very first order came through.

`pixel_only` is a website order that arrived with no tags, only a pixel's
cookie. Measured on production 2026-10-28: of 1 616 such buyers over 180 days
38% had also bought through organic and 21% through ads, and in August 2026,
when the comment template lost its UTM, the bucket rose from ~30% of website
orders to 70%. These tests hold the rule that says who those people are:
"first" is Silver's own rule for a new buyer — every source, every sales
type, returns excluded, ties by id.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pytest

from core.duckdb_store import DuckDBStore
from core.utm_classify import FIRST_CHANNEL_OTHER, classify_traffic, first_order_channel

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


async def _store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "pixel.duckdb")
    await s.connect()
    return s


async def _order(store: DuckDBStore, order_id: int, buyer_id: Optional[int], *,
                 day: date = DAY, total: float = 100.0, source_id: int = 4,
                 utm: Optional[dict] = None, new: bool = False,
                 is_return: bool = False, active: bool = True) -> None:
    async with store.connection() as conn:
        conn.execute(
            """
            INSERT INTO silver_orders (
                id, source_id, status_id, grand_total, ordered_at, buyer_id,
                manager_id, order_date, is_return, sales_type, is_active_source,
                source_name, is_new_customer, buyer_first_order_date, promocode
            ) VALUES (?, ?, 12, ?, NULL, ?, NULL, ?, ?, 'retail', ?, 'x', ?, NULL, NULL)
            """,
            [order_id, source_id, total, buyer_id, day, is_return, active, new],
        )
        if utm is not None:
            tt, platform = classify_traffic(utm)
            conn.execute(
                "INSERT INTO silver_order_utm (order_id, utm_source, utm_medium,"
                " utm_campaign, utm_content, fbp, traffic_type, platform, parsed_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)",
                [order_id, utm.get("utm_source"), utm.get("utm_medium"),
                 utm.get("utm_campaign"), utm.get("utm_content"),
                 utm.get("_fbp"), tt, platform],
            )


def _firsts(out: dict) -> dict:
    return {(r["group"], r["channel"]): r for r in out["first_channels"]}


class TestTheBreakdown:

    @pytest.mark.asyncio
    async def test_returning_buyers_are_placed_by_their_first_order(self, tmp_path):
        """Buyer 1 first ordered in Instagram Direct, buyer 2 through the
        profile link, buyer 3 through a Facebook ad; each comes back to the
        website with no tags. Buyer 4's first order is this pixel order."""
        store = await _store(tmp_path)
        before = DAY - timedelta(days=60)
        try:
            await _order(store, 11, 1, day=before, source_id=1)
            await _order(store, 21, 2, day=before, utm=BIO)
            await _order(store, 31, 3, day=before, utm=FB_AD)
            await _order(store, 12, 1, total=300, utm=PIXEL)
            await _order(store, 22, 2, total=200, utm=PIXEL)
            await _order(store, 32, 3, total=150, utm=PIXEL)
            await _order(store, 41, 4, total=120, utm=PIXEL, new=True)
            out = await store.get_traffic_pixel_only(*WINDOW, sales_type="retail")
        finally:
            await store.close()

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
    async def test_first_counts_every_source_and_skips_returns(self, tmp_path):
        """Silver's rule: buyer 5's first order was on the 2024 website
        (an inactive source), which still makes it the first; buyer 6's
        earliest order was cancelled, so the next one is the first."""
        store = await _store(tmp_path)
        try:
            await _order(store, 51, 5, day=DAY - timedelta(days=400),
                         source_id=3, active=False)
            await _order(store, 52, 5, utm=PIXEL)
            await _order(store, 61, 6, day=DAY - timedelta(days=90),
                         utm=FB_AD, is_return=True)
            await _order(store, 62, 6, day=DAY - timedelta(days=30), utm=BIO)
            await _order(store, 63, 6, utm=PIXEL)
            out = await store.get_traffic_pixel_only(*WINDOW, sales_type="retail")
        finally:
            await store.close()

        assert set(_firsts(out)) == {("unknown", "opencart"), ("organic", "instagram_site")}

    @pytest.mark.asyncio
    async def test_the_total_is_the_pixel_only_card(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await _order(store, 1, 1, total=300, utm=PIXEL, new=True)
            await _order(store, 2, 2, total=200, utm=PIXEL)
            await _order(store, 3, 3, total=999, utm=FB_AD)
            await _order(store, 4, None, total=50, utm=PIXEL)
            out = await store.get_traffic_pixel_only(*WINDOW, sales_type="retail")
            card = (await store.get_traffic_analytics(
                *WINDOW, sales_type="retail"))["summary"]["pixel_only"]
        finally:
            await store.close()

        assert {k: out["totals"][k] for k in ("orders", "revenue")} == card
        assert ("unknown", "unknown") in _firsts(out)

    @pytest.mark.asyncio
    async def test_an_empty_window(self, tmp_path):
        store = await _store(tmp_path)
        try:
            out = await store.get_traffic_pixel_only(*WINDOW, sales_type="retail")
        finally:
            await store.close()

        assert out["first_channels"] == []
        assert out["totals"] == {"orders": 0, "revenue": 0.0, "buyers": 0}
