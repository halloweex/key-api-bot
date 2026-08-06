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
async def test_delivery_counts_are_reported(tmp_path):
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=4, holdouts=2)

    async with store.connection() as conn:
        conn.execute("UPDATE sms_campaign_members SET message_id='m1',"
                     " delivered=TRUE WHERE campaign='aug' AND buyer_id=1")
        conn.execute("UPDATE sms_campaign_members SET delivered=FALSE"
                     " WHERE campaign='aug' AND buyer_id IN (2,3)")

    target = (await store.get_sms_campaign_results("aug"))["overall"]["target"]

    assert target["contacts"] == 4
    assert target["delivered"] == 1
    assert target["undelivered"] == 2
    # buyer 4 has no report yet — neither delivered nor failed
    assert target["delivered"] + target["undelivered"] < target["contacts"]

    await store.close()


@pytest.mark.asyncio
async def test_delivered_only_never_filters_the_control(tmp_path):
    """The control was never sent to, so a delivery filter must not touch it.

    Filtering it would strip the baseline and turn any campaign into a success.
    """
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=10, holdouts=10)

    async with store.connection() as conn:
        # Only half the target arm was delivered to; the control has no reports
        conn.execute("UPDATE sms_campaign_members SET delivered=TRUE"
                     " WHERE campaign='aug' AND buyer_id <= 5")
        conn.execute("UPDATE sms_campaign_members SET delivered=FALSE"
                     " WHERE campaign='aug' AND buyer_id BETWEEN 6 AND 10")

    for i in (1, 2, 3):
        await _buy(store, i, days_after=3)
    for i in (11, 12):
        await _buy(store, i, days_after=3)

    itt = (await store.get_sms_campaign_results("aug"))["overall"]
    assert itt["target"]["contacts"] == 10
    assert itt["holdout"]["contacts"] == 10

    restricted = await store.get_sms_campaign_results("aug", delivered_only=True)
    assert restricted["deliveredOnly"] is True
    assert restricted["overall"]["target"]["contacts"] == 5, "target is restricted"
    assert restricted["overall"]["holdout"]["contacts"] == 10, \
        "the control arm keeps every member"

    # Same three buyers either way, but the target rate rises because the
    # denominator shrank — exactly why this reading is a bound, not the result.
    assert itt["comparison"]["conversionTarget"] == 30.0
    assert restricted["overall"]["comparison"]["conversionTarget"] == 60.0

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


# ─── the window starts at the send, not at midnight ──────────────────────

async def _buy_at(store, buyer_id: int, when: datetime, oid: int,
                  total: str = "1000.00"):
    """A purchase at a precise moment, for testing the window's edges."""
    async with store.connection() as conn:
        conn.execute(
            """
            INSERT INTO silver_orders (
                id, source_id, status_id, grand_total, ordered_at, buyer_id,
                manager_id, order_date, is_return, sales_type, is_active_source,
                source_name, is_new_customer, promocode
            ) VALUES (?, 4, 1, ?, ?, ?, NULL, ?, FALSE, 'retail', TRUE,
                      'Shopify', FALSE, NULL)
            """,
            [oid, total, when, buyer_id, when.date()],
        )
        conn.execute(
            "INSERT INTO order_products (id, order_id, product_id, name, quantity,"
            " price_sold) VALUES (?, ?, 1, 'Cream', 1, ?)",
            [oid, oid, total],
        )


@pytest.mark.asyncio
async def test_a_purchase_before_the_send_is_not_the_campaign(tmp_path):
    """Rounding the start down to a date credited the campaign with a whole
    day of ordinary trading that happened before anyone was messaged."""
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=10, holdouts=10)

    # SENT is 10:00. This one bought at 09:00, an hour earlier.
    await _buy_at(store, 1, SENT - timedelta(hours=1), oid=900)

    overall = (await store.get_sms_campaign_results("aug"))["overall"]
    assert overall["target"]["converted"] == 0

    await store.close()


@pytest.mark.asyncio
async def test_a_purchase_after_the_send_counts_the_same_day(tmp_path):
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=10, holdouts=10)

    await _buy_at(store, 1, SENT + timedelta(minutes=30), oid=901)

    overall = (await store.get_sms_campaign_results("aug"))["overall"]
    assert overall["target"]["converted"] == 1

    await store.close()


@pytest.mark.asyncio
async def test_the_window_closes_exactly_n_days_after_the_send(tmp_path):
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=10, holdouts=10)

    await _buy_at(store, 1, SENT + timedelta(days=7) - timedelta(minutes=1), oid=902)
    await _buy_at(store, 2, SENT + timedelta(days=7) + timedelta(minutes=1), oid=903)

    overall = (await store.get_sms_campaign_results("aug", window_days=7))["overall"]
    assert overall["target"]["converted"] == 1, "only the one inside the window"

    await store.close()


@pytest.mark.asyncio
async def test_members_never_handed_to_the_gateway_leave_the_arm(tmp_path):
    """They cannot respond to a message they never got, so counting them as
    contacts understates the rate on every reading."""
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=10, holdouts=10)
    async with store.connection() as conn:
        conn.execute(
            "UPDATE sms_campaign_members SET delivery_status = 'NotSent',"
            " delivered = FALSE WHERE campaign = 'aug' AND buyer_id IN (1, 2)"
        )

    overall = (await store.get_sms_campaign_results("aug"))["overall"]

    assert overall["target"]["contacts"] == 8, "only the ones actually messaged"
    assert overall["target"]["notSent"] == 2, "but the loss stays visible"
    assert overall["holdout"]["contacts"] == 10, "the control is untouched"
    assert overall["holdout"]["notSent"] == 0

    await store.close()


@pytest.mark.asyncio
async def test_a_purchase_by_someone_never_messaged_is_not_a_response(tmp_path):
    """They bought without ever seeing the message, so it is not the campaign's."""
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=10, holdouts=10)
    async with store.connection() as conn:
        conn.execute(
            "UPDATE sms_campaign_members SET delivery_status = 'NotSent',"
            " delivered = FALSE WHERE campaign = 'aug' AND buyer_id = 1"
        )
    await _buy(store, 1, days_after=2)
    await _buy(store, 2, days_after=2)

    overall = (await store.get_sms_campaign_results("aug"))["overall"]

    assert overall["target"]["converted"] == 1, "buyer 2 only"
    assert overall["target"]["contacts"] == 9

    await store.close()


async def _second_record(store, *, buyer_id: int, phone: str, name: str):
    """A duplicate customer row for the same person, reached on the same phone.

    Responding to a campaign creates these: the recipient follows the link,
    checks out on the storefront, and the name is spelled differently enough
    that a fresh buyer row is written.
    """
    async with store.connection() as conn:
        conn.execute(
            "INSERT INTO buyers (id, full_name, phone) VALUES (?, ?, ?)",
            [buyer_id, name, phone],
        )


@pytest.mark.asyncio
async def test_an_empty_control_does_not_certify_an_effect(tmp_path):
    """The bug this guard exists for.

    With Wald, a control arm where nobody has bought contributes exactly zero
    variance, so the interval clears zero and the campaign reads as proven on
    its first day. Nothing has been shown: 0 purchases out of 160 is consistent
    with a true rate of nearly 2%, which is the whole of the measured lift.
    """
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=1350, holdouts=160)

    for i in range(1, 26):            # 25/1350 target, 0/160 control
        await _buy(store, i, days_after=1)

    cmp = (await store.get_sms_campaign_results("aug"))["overall"]["comparison"]

    assert cmp["conversionHoldout"] == 0.0
    assert cmp["liftPp"] > 1.8, "the raw gap is real enough to be tempting"
    assert cmp["significant"] is False, "but an empty control shows nothing"
    assert cmp["verdictReady"] is False
    assert cmp["ci95Pp"][0] < 0, "the interval must reach below zero"
    assert cmp["pValue"] > 0.05

    await store.close()


@pytest.mark.asyncio
async def test_a_verdict_needs_purchases_in_both_arms(tmp_path):
    """Widening the interval is not enough on its own.

    At one purchase in a large control the interval still clears zero, and the
    honest reading of that is "too early", not "proven".
    """
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=2000, holdouts=600)

    for i in range(1, 41):
        await _buy(store, i, days_after=1)
    await _buy(store, 2001, days_after=1)      # a single control purchase

    cmp = (await store.get_sms_campaign_results("aug"))["overall"]["comparison"]

    assert cmp["eventsHoldout"] == 1
    assert cmp["minEvents"] == 5
    assert cmp["verdictReady"] is False
    assert cmp["significant"] is False

    await store.close()


@pytest.mark.asyncio
async def test_a_verdict_is_offered_once_both_arms_have_bought(tmp_path):
    """The guard must not swallow a result that has genuinely arrived."""
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=1000, holdouts=1000)

    for i in range(1, 121):                    # 12% target
        await _buy(store, i, days_after=2)
    for i in range(1001, 1041):                # 4% control
        await _buy(store, i, days_after=2)

    cmp = (await store.get_sms_campaign_results("aug"))["overall"]["comparison"]

    assert cmp["verdictReady"] is True
    assert cmp["significant"] is True
    assert cmp["ci95Pp"][0] > 0

    await store.close()


@pytest.mark.asyncio
async def test_the_interval_holds_the_lift_it_is_drawn_around(tmp_path):
    """The chart draws the point and the interval from these two fields."""
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=500, holdouts=200)

    for i in range(1, 51):
        await _buy(store, i, days_after=2)
    for i in range(501, 511):
        await _buy(store, i, days_after=2)

    cmp = (await store.get_sms_campaign_results("aug"))["overall"]["comparison"]

    lo, hi = cmp["ci95Pp"]
    assert lo <= cmp["liftPp"] <= hi

    await store.close()


@pytest.mark.asyncio
async def test_a_response_under_a_second_customer_record_still_counts(tmp_path):
    """The campaign's own doing: the click creates the duplicate record.

    Matching the purchase back by buyer_id misses it, and only the messaged arm
    holds a link to click — so the loss is one-sided and always understates the
    campaign.
    """
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=10, holdouts=10)

    # Member 3's phone is 380900000003 (see _seed_campaign); she checks out as
    # a new customer, and the storefront writes buyer 9003 with a '+' prefix.
    await _second_record(store, buyer_id=9003, phone="+380900000003",
                         name="Anastasiia Y.")
    await _buy(store, 9003, days_after=1, total="2000.00")

    overall = (await store.get_sms_campaign_results("aug"))["overall"]

    assert overall["target"]["converted"] == 1, "she is one responder, not none"
    assert overall["target"]["revenue"] == 2000.0
    assert overall["target"]["contacts"] == 10, "and still one contact"

    await store.close()


@pytest.mark.asyncio
async def test_one_person_on_two_records_is_still_one_response(tmp_path):
    """Rolling records up must not turn one buyer into two."""
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=10, holdouts=10)

    await _second_record(store, buyer_id=9003, phone="380900000003",
                         name="Duplicate")
    await _buy(store, 3, days_after=1, total="1000.00")
    await _buy(store, 9003, days_after=2, total="1000.00")

    overall = (await store.get_sms_campaign_results("aug"))["overall"]

    assert overall["target"]["converted"] == 1, "one person"
    assert overall["target"]["orders"] == 2, "who placed two orders"
    assert overall["target"]["revenue"] == 2000.0

    await store.close()


@pytest.mark.asyncio
async def test_a_stranger_on_a_different_number_is_not_a_member(tmp_path):
    """The phone match must not sweep in whoever happens to be nearby."""
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=10, holdouts=10)

    await _second_record(store, buyer_id=9999, phone="380671234567",
                         name="Someone Else")
    await _buy(store, 9999, days_after=1)

    overall = (await store.get_sms_campaign_results("aug"))["overall"]

    assert overall["target"]["converted"] == 0
    assert overall["holdout"]["converted"] == 0

    await store.close()


@pytest.mark.asyncio
async def test_delivery_filter_keeps_recipients_with_no_report(tmp_path):
    """A receipt that never arrived is not a failed delivery.

    The provider's webhook does not reach us, so most recipients keep a NULL
    delivery flag for ever. Reading NULL as undelivered dropped 246 people from
    the first real campaign against 25 actual refusals.
    """
    store = await _make_store(tmp_path)
    await _seed_campaign(store, targets=10, holdouts=10)
    async with store.connection() as conn:
        conn.execute("UPDATE sms_campaign_members SET delivered = TRUE,"
                     " delivery_status = 'Delivered'"
                     " WHERE campaign = 'aug' AND buyer_id <= 3")
        conn.execute("UPDATE sms_campaign_members SET delivered = FALSE,"
                     " delivery_status = 'Rejected'"
                     " WHERE campaign = 'aug' AND buyer_id = 4")
        # 5..10 keep delivery_status 'Sent' and a NULL flag — in transit.

    restricted = (await store.get_sms_campaign_results(
        "aug", delivered_only=True))["overall"]

    assert restricted["target"]["contacts"] == 9, "only the refusal is dropped"
    assert restricted["holdout"]["contacts"] == 10

    await store.close()
