"""Tests for measuring an SMS campaign against its holdout.

Each test plants a known response pattern and checks what comes back, so the
arithmetic is verified against outcomes we chose rather than against itself.
"""
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore

SENT = datetime(2026, 8, 1, 10, 0)
SENT_DATE = date(2026, 8, 1)


async def _make_store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await store.connect()
    async with store.connection() as conn:
        conn.execute(
            "INSERT INTO products (id, name, sku, price) VALUES (1, 'Cream', 'SKU-1', 1000)"
        )
        conn.execute(
            "INSERT INTO offer_stocks (id, sku, price, purchased_price, quantity)"
            " VALUES (1, 'SKU-1', 1000, 400, 100)"
        )
    return store


async def _seed_campaign(store, *, targets: int, holdouts: int, tier: str = "CORE"):
    """Freeze a roster of synthetic members; buyer ids are 1..n."""
    members = []
    for i in range(1, targets + holdouts + 1):
        members.append({
            "buyerId": i,
            "phone": f"3809{i:08d}",
            "tier": tier,
            "assignment": "target" if i <= targets else "holdout",
            "orders": 2,
            "revenueLtv": 5000.0,
            "marginLtv": 3000.0,
            "recencyDays": 30,
        })
    await store.freeze_sms_campaign(
        campaign="aug", customers=members, criteria={}, ltv_basis="margin",
        sales_type="retail", holdout_pct=10, promocode="KS-AUG",
    )
    await store.mark_sms_campaign_sent("aug", SENT)


async def _buy(store, buyer_id: int, *, days_after: int, total: str = "1000.00",
               promocode=None, oid=None):
    order_date = SENT_DATE + timedelta(days=days_after)
    oid = oid if oid is not None else 10000 + buyer_id * 10 + days_after
    async with store.connection() as conn:
        conn.execute(
            """
            INSERT INTO silver_orders (
                id, source_id, status_id, grand_total, ordered_at, buyer_id,
                manager_id, order_date, is_return, sales_type, is_active_source,
                source_name, is_new_customer, promocode
            ) VALUES (?, 4, 1, ?, ?, ?, NULL, ?, FALSE, 'retail', TRUE,
                      'Shopify', FALSE, ?)
            """,
            [oid, total, order_date, buyer_id, order_date, promocode],
        )
        conn.execute(
            "INSERT INTO order_products (id, order_id, product_id, name, quantity,"
            " price_sold) VALUES (?, ?, 1, 'Cream', 1, ?)",
            [oid, oid, total],
        )


@pytest.mark.asyncio
async def test_lift_is_the_difference_not_the_raw_rate(tmp_path):
    """40% of target buy, but so do 20% of holdout — the result is 20pp, not 40%."""
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=100, holdouts=100)

    for i in range(1, 41):            # 40/100 target
        await _buy(store, i, days_after=5)
    for i in range(101, 121):         # 20/100 holdout
        await _buy(store, i, days_after=5)

    cmp = (await store.get_sms_campaign_results("aug"))["overall"]["comparison"]

    assert cmp["conversionTarget"] == 40.0
    assert cmp["conversionHoldout"] == 20.0
    assert cmp["liftPp"] == 20.0
    assert cmp["liftRelativePct"] == 100.0
    assert cmp["significant"] is True
    assert cmp["pValue"] < 0.01

    await store.close()


@pytest.mark.asyncio
async def test_no_real_effect_is_reported_as_not_significant(tmp_path):
    """Equal rates: the interval must span zero and say so."""
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=100, holdouts=100)

    for i in range(1, 31):
        await _buy(store, i, days_after=3)
    for i in range(101, 131):
        await _buy(store, i, days_after=3)

    cmp = (await store.get_sms_campaign_results("aug"))["overall"]["comparison"]

    assert cmp["liftPp"] == 0.0
    assert cmp["significant"] is False
    assert cmp["ci95Pp"][0] < 0 < cmp["ci95Pp"][1]
    assert cmp["pValue"] > 0.5

    await store.close()


@pytest.mark.asyncio
async def test_small_holdout_widens_the_interval(tmp_path):
    """A visible gap on a tiny control is not evidence, and must not read as one."""
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=100, holdouts=10)

    for i in range(1, 31):            # 30% target
        await _buy(store, i, days_after=4)
    await _buy(store, 101, days_after=4)   # 10% holdout

    cmp = (await store.get_sms_campaign_results("aug"))["overall"]["comparison"]

    assert cmp["liftPp"] == 20.0, "the raw gap looks large"
    assert cmp["significant"] is False, "but 10 controls cannot establish it"
    assert cmp["ci95Pp"][0] < 0, "the interval reaches below zero"

    await store.close()


@pytest.mark.asyncio
async def test_purchases_outside_the_window_are_ignored(tmp_path):
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=10, holdouts=10)

    await _buy(store, 1, days_after=5)     # inside
    await _buy(store, 2, days_after=29)    # inside
    await _buy(store, 3, days_after=31)    # past the 30-day window
    await _buy(store, 4, days_after=-3)    # before the send

    target = (await store.get_sms_campaign_results("aug"))["overall"]["target"]
    assert target["converted"] == 2

    wider = await store.get_sms_campaign_results("aug", window_days=60)
    assert wider["overall"]["target"]["converted"] == 3, "wider window catches day 31"
    assert wider["windowDays"] == 60

    await store.close()


@pytest.mark.asyncio
async def test_incremental_money_is_per_contact_difference(tmp_path):
    """Revenue the campaign added, not revenue the group happened to spend."""
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=10, holdouts=10)

    for i in range(1, 6):             # 5 target buyers x 1000 = 5000 -> 500/contact
        await _buy(store, i, days_after=2)
    for i in range(11, 13):           # 2 holdout buyers x 1000 = 2000 -> 200/contact
        await _buy(store, i, days_after=2)

    cmp = (await store.get_sms_campaign_results("aug"))["overall"]["comparison"]

    assert cmp["incrementalRevenuePerContact"] == 300.0
    assert cmp["incrementalRevenueTotal"] == 3000.0
    # Cost is 400 of every 1000, so margin per contact moves 60% as much
    assert cmp["incrementalMarginPerContact"] == 180.0
    assert cmp["incrementalMarginTotal"] == 1800.0

    await store.close()


@pytest.mark.asyncio
async def test_promocode_orders_counted_separately(tmp_path):
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=10, holdouts=10)

    await _buy(store, 1, days_after=2, promocode="KS-AUG")
    await _buy(store, 2, days_after=2, promocode="KS-AUG")
    await _buy(store, 3, days_after=2, promocode="OTHER")
    await _buy(store, 4, days_after=2)

    result = await store.get_sms_campaign_results("aug")

    assert result["promocode"] == "KS-AUG"
    assert result["overall"]["target"]["promoOrders"] == 2
    assert result["overall"]["target"]["converted"] == 4, "all four still bought"

    await store.close()


@pytest.mark.asyncio
async def test_results_broken_down_by_tier(tmp_path):
    store = await _make_store(tmp_path)
    members = []
    for i in range(1, 21):
        members.append({
            "buyerId": i, "phone": f"3809{i:08d}",
            "tier": "VIP" if i <= 10 else "REACTIVATION",
            "assignment": "target" if i % 10 < 8 and i % 10 != 0 else "holdout",
            "orders": 2, "revenueLtv": 5000.0, "marginLtv": 3000.0, "recencyDays": 30,
        })
    await store.freeze_sms_campaign(
        campaign="aug", customers=members, criteria={}, ltv_basis="margin",
        sales_type="retail", holdout_pct=20,
    )
    await store.mark_sms_campaign_sent("aug", SENT)
    await _buy(store, 1, days_after=1)
    await _buy(store, 11, days_after=1)

    result = await store.get_sms_campaign_results("aug")
    tiers = {s["tier"]: s for s in result["segments"]}

    assert [s["tier"] for s in result["segments"]] == ["VIP", "REACTIVATION"]
    assert tiers["VIP"]["target"]["converted"] == 1
    assert tiers["REACTIVATION"]["target"]["converted"] == 1
    assert result["overall"]["target"]["converted"] == 2

    await store.close()


@pytest.mark.asyncio
async def test_only_roster_members_are_counted(tmp_path):
    """A customer who wasn't in the campaign cannot contribute to its result."""
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=5, holdouts=5)

    await _buy(store, 1, days_after=2)
    await _buy(store, 999, days_after=2)   # never in the roster

    overall = (await store.get_sms_campaign_results("aug"))["overall"]
    assert overall["target"]["contacts"] == 5
    assert overall["target"]["converted"] == 1

    await store.close()


@pytest.mark.asyncio
async def test_returns_and_dead_sources_excluded(tmp_path):
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=10, holdouts=10)

    await _buy(store, 1, days_after=2)
    async with store.connection() as conn:
        for oid, buyer, is_return, active in ((900, 2, True, True), (901, 3, False, False)):
            conn.execute(
                """
                INSERT INTO silver_orders (
                    id, source_id, status_id, grand_total, ordered_at, buyer_id,
                    manager_id, order_date, is_return, sales_type, is_active_source,
                    source_name, is_new_customer
                ) VALUES (?, 4, 1, '1000.00', ?, ?, NULL, ?, ?, 'retail', ?,
                          'Shopify', FALSE)
                """,
                [oid, SENT_DATE + timedelta(days=2), buyer,
                 SENT_DATE + timedelta(days=2), is_return, active],
            )
            conn.execute(
                "INSERT INTO order_products (id, order_id, product_id, name, quantity,"
                " price_sold) VALUES (?, ?, 1, 'Cream', 1, '1000.00')", [oid, oid],
            )

    overall = (await store.get_sms_campaign_results("aug"))["overall"]
    assert overall["target"]["converted"] == 1

    await store.close()


@pytest.mark.asyncio
async def test_unsent_campaign_refuses_to_report(tmp_path):
    store = await _make_store(tmp_path)
    await store.freeze_sms_campaign(
        campaign="draft",
        customers=[{"buyerId": 1, "phone": "380961111111", "tier": "CORE",
                    "assignment": "target", "orders": 1, "revenueLtv": 100.0,
                    "marginLtv": 50.0, "recencyDays": 10}],
        criteria={}, ltv_basis="revenue", sales_type="retail", holdout_pct=10,
    )

    with pytest.raises(ValueError, match="no send date"):
        await store.get_sms_campaign_results("draft")

    await store.close()


@pytest.mark.asyncio
async def test_unknown_campaign(tmp_path):
    store = await _make_store(tmp_path)
    with pytest.raises(ValueError, match="not frozen"):
        await store.get_sms_campaign_results("nope")
    await store.close()
