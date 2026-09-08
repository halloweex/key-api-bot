"""What the tab's payload may claim, and what it may not claim out of nothing.

Two defects of the same shape, found reading a live response rather than the
code. `bonus_tier` defaulted to the bottom tier, so a period with no ad spend
recorded — which is every period, `manual_expenses` holds no rows in
production — put "No bonus" on the card and lit the "< 4.0x" row of the tier
table as the one in force. And it was an English display string that the
frontend then string-compared against its own translated label, so the
highlight only ever matched in English.

The other is small and in the same dict: five of the seven summary buckets
were rounded to the kopeck and two were handed straight out of the
accumulator, so the payload carried 1182863.0299999998 beside 1182863.03.
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore
from core.repositories.traffic import TrafficMixin

DAY = date(2026, 8, 12)
WINDOW = (DAY - timedelta(days=1), DAY + timedelta(days=1))


async def _store(tmp_path: Path) -> DuckDBStore:
    s = DuckDBStore(db_path=tmp_path / "payload.duckdb")
    await s.connect()
    return s


async def _order(store: DuckDBStore, order_id: int, total: float,
                 source_id: int = 4) -> None:
    async with store.connection() as conn:
        conn.execute(
            """
            INSERT INTO silver_orders (
                id, source_id, status_id, grand_total, ordered_at, buyer_id,
                manager_id, order_date, is_return, sales_type, is_active_source,
                source_name, is_new_customer, buyer_first_order_date, promocode
            ) VALUES (?, ?, 12, ?, NULL, NULL, NULL, ?, FALSE, 'retail',
                      TRUE, 'Shopify', FALSE, NULL, NULL)
            """,
            [order_id, source_id, total, DAY],
        )


async def _gold(store: DuckDBStore, revenue: float) -> None:
    """The revenue Gold is what blended ROAS divides; ROAS does not read
    `silver_orders` for its numerator."""
    async with store.connection() as conn:
        conn.execute(
            "INSERT INTO gold_daily_revenue (date, sales_type, revenue) "
            "VALUES (?, 'retail', ?)",
            [DAY, revenue],
        )


async def _spend(store: DuckDBStore, amount: float,
                 platform: str = "facebook") -> None:
    async with store.connection() as conn:
        conn.execute(
            "INSERT INTO manual_expenses (expense_date, category, expense_type,"
            " amount, currency, note, platform)"
            " VALUES (?, 'marketing', 'Facebook Ads', ?, 'UAH', NULL, ?)",
            [DAY, amount, platform],
        )


async def _roas(store: DuckDBStore) -> dict:
    return await store.get_traffic_roas(
        start_date=WINDOW[0], end_date=WINDOW[1], sales_type="retail")


class TestTheBonusTierIsAVerdictOrNothing:

    @pytest.mark.asyncio
    async def test_no_spend_recorded_means_no_tier(self, tmp_path):
        """The production case. Revenue exists, spend does not, so there is
        no ratio — and therefore no tier. It used to answer "No bonus"."""
        store = await _store(tmp_path)
        await _order(store, 1, 1_000.0)
        await _gold(store, 1_000.0)

        out = await _roas(store)
        assert out["blended"]["roas"] is None
        assert out["bonus_tier"] is None
        assert out["has_spend_data"] is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize("revenue,spend,expected", [
        (8_000.0, 1_000.0, "plus_30"),   # 8.0x
        (7_000.0, 1_000.0, "plus_30"),   # exactly at the boundary
        (6_500.0, 1_000.0, "plus_20"),
        (5_500.0, 1_000.0, "plus_10"),
        (4_500.0, 1_000.0, "base"),
        (1_000.0, 1_000.0, "none"),
    ])
    async def test_a_real_ratio_lands_in_a_tier(self, tmp_path, revenue, spend,
                                                expected):
        store = await _store(tmp_path)
        await _order(store, 1, revenue)
        await _gold(store, revenue)
        await _spend(store, spend)

        out = await _roas(store)
        assert out["bonus_tier"] == expected

    @pytest.mark.asyncio
    async def test_the_tier_is_a_key_and_never_a_sentence(self, tmp_path):
        """A display string here has to be translated by whoever renders it,
        and the renderer cannot translate a value it also has to compare
        against. Keys are the only thing both halves can agree on."""
        store = await _store(tmp_path)
        await _order(store, 1, 9_000.0)
        await _gold(store, 9_000.0)
        await _spend(store, 1_000.0)

        out = await _roas(store)
        assert out["bonus_tier"] in {k for _, k in TrafficMixin.BONUS_TIERS}
        assert " " not in out["bonus_tier"], "a key, not a label"
        assert "%" not in out["bonus_tier"]


class TestEveryBucketIsRoundedTheSameWay:

    @pytest.mark.asyncio
    async def test_all_seven_summary_buckets_carry_two_decimals(self, tmp_path):
        """Three thirds of a hryvnia in each bucket: the sum is periodic in
        binary, so an unrounded accumulator shows it."""
        store = await _store(tmp_path)
        # One order per traffic type the fallback and the UTM rows can produce.
        await _order(store, 1, 10.01, source_id=1)   # organic (no UTM row)
        await _order(store, 2, 10.01, source_id=4)   # unknown (no UTM row)
        for oid, total, tt, pf in (
            (3, 10.01, "paid_confirmed", "facebook"),
            (4, 10.01, "paid_likely", "facebook"),
            (5, 10.01, "manager", "manager"),
            (6, 10.01, "pixel_only", "other"),
        ):
            await _order(store, oid, total)
            async with store.connection() as conn:
                conn.execute(
                    "INSERT INTO silver_order_utm (order_id, traffic_type,"
                    " platform, parsed_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                    [oid, tt, pf],
                )
        # Three more into each of the two buckets that used to skip rounding,
        # so their totals are 10.01 * 4 and cannot be exact in binary.
        for oid, source_id in ((7, 4), (8, 4), (9, 4)):
            await _order(store, oid, 10.01, source_id=source_id)
            async with store.connection() as conn:
                conn.execute(
                    "INSERT INTO silver_order_utm (order_id, traffic_type,"
                    " platform, parsed_at) VALUES (?, 'pixel_only', 'other',"
                    " CURRENT_TIMESTAMP)",
                    [oid],
                )

        out = await store.get_traffic_analytics(
            start_date=WINDOW[0], end_date=WINDOW[1], sales_type="retail")

        for name, bucket in out["summary"].items():
            revenue = bucket["revenue"]
            assert round(revenue, 2) == revenue, f"{name} is unrounded: {revenue!r}"
            assert isinstance(bucket["orders"], int), name

    @pytest.mark.asyncio
    async def test_an_absent_bucket_is_zero_and_not_missing(self, tmp_path):
        """The frontend reads all five cards unconditionally."""
        store = await _store(tmp_path)
        await _order(store, 1, 100.0, source_id=1)

        out = await store.get_traffic_analytics(
            start_date=WINDOW[0], end_date=WINDOW[1], sales_type="retail")

        for name in ("paid", "paid_confirmed", "paid_likely", "organic",
                     "manager", "pixel_only", "unknown"):
            assert out["summary"][name] == {
                "orders": out["summary"][name]["orders"],
                "revenue": out["summary"][name]["revenue"],
            }
        assert out["summary"]["pixel_only"] == {"orders": 0, "revenue": 0.0}
        assert out["summary"]["unknown"] == {"orders": 0, "revenue": 0.0}


class TestBeingToldNothingIsNotTheSameAsNotUnderstanding:
    """`other` and `unattributed` are two different ignorances.

    `other` is a source we were told and have not learned to name — `qr`,
    `novaposhta`, `rivo`. `unattributed` is having been told nothing: our own
    pixels fired and no parameter says where the buyer came from. They shared
    the key `other` until 2026-09-09, which put ₴1.36M of "we don't know"
    under a word that reads as "miscellaneous small channels".
    """

    def test_a_pixel_is_not_a_channel(self):
        assert TrafficMixin._classify_traffic({"_fbp": "fb.1"}) \
            == ("pixel_only", "unattributed")
        assert TrafficMixin._classify_traffic({"ttp": "T"}) \
            == ("pixel_only", "unattributed")
        assert TrafficMixin._classify_traffic({"_fbp": "fb.1", "ttp": "T"}) \
            == ("pixel_only", "unattributed")

    def test_nothing_at_all_is_not_a_channel(self):
        assert TrafficMixin._classify_traffic({}) == ("unknown", "unattributed")

    def test_a_source_we_were_told_but_cannot_place_stays_other(self):
        """The distinction the split exists for: these carry a real tag, so
        somebody could map them one day. `qr` and `novaposhta` are live
        examples from production."""
        for source in ("qr", "novaposhta", "rivo", "wishpicks"):
            traffic_type, platform = TrafficMixin._classify_traffic(
                {"utm_source": source, "utm_medium": "referral"})
            assert platform == "other", f"{source} lost its tag"
            assert traffic_type == "organic"

    def test_the_fallback_for_an_order_with_no_row_agrees(self):
        """`_PLATFORM_EXPR` is what places an order the parser never saw. It
        has to reach the same verdict as the classifier or the chart carries
        two names for one state."""
        assert "ELSE 'unattributed' END" in TrafficMixin._PLATFORM_EXPR
        assert "'other'" not in TrafficMixin._PLATFORM_EXPR

    @pytest.mark.parametrize("lang", ["en", "uk", "ru"])
    def test_the_report_can_name_it(self, lang):
        """It falls back to the raw key when a platform has no translation,
        so an untranslated one reaches a human as `unattributed`."""
        from core.traffic_report import _platform_label

        label = _platform_label("unattributed", lang)
        assert label != "unattributed", f"{lang} is showing the raw key"
        assert _platform_label("other", lang) != label, "other must stay distinct"
