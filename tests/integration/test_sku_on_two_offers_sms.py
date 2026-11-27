"""A SKU on two offers is costed once, in the SMS audience and its results.

`bronze.offer_stocks` is unique only on an offer's id: a SKU can sit on two
offers, and a query that joins the table straight on `sku` repeats every line
of that SKU, both costs taken off its margin. The Rust port of /margin found
it there (`tests/integration/test_margin_defects.py` holds /margin); the SMS
audience and the campaign results joined the same way, and were fixed to cost
a SKU once, at its highest cost, as /margin does. Revenue is spread over an
order's lines pro rata, so a repeated line does not raise it — the margin is
what the repeat breaks, and with it a ranking by margin and the campaign's
reported profit.

One buyer, two orders of one basket, ₴1 500 each: SKU-DUP × 2 at 500 (on two
offers, costs 300 and 400) and SKU-ONE × 1 at 500 (cost 100). An order's
margin at the highest cost is 1 500 − 2 × 400 − 100 = ₴600. The campaign went
out between the two orders, so its results hold the second one alone.
"""
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
import pytest_asyncio

from tests.helpers.app_client import signed_in
from tests.integration import pg_sms_seed
from tests.integration.pg_sms_seed import insert, kyiv_today, needs_postgres, order_row

pytestmark = needs_postgres

KYIV = ZoneInfo("Europe/Kyiv")
SEGMENTS = "/api/customers/sms-segments"
CAMPAIGNS = "/api/customers/sms-campaigns"

BUYER = 1
# (order, days ago); the campaign is marked sent between them.
ORDERS = [(11, 20), (12, 5)]
SENT_DAYS_AGO = 10


async def _populate(conn) -> None:
    await insert(conn, "bronze.categories", [
        dict(id=10, name="Face care", parent_id=None)])
    await insert(conn, "bronze.products", [
        dict(id=100, name="Toner", sku="SKU-DUP", brand="Cosrx", category_id=10),
        dict(id=101, name="Serum", sku="SKU-ONE", brand="Anua", category_id=10),
    ])
    await insert(conn, "bronze.offer_stocks", [
        dict(id=1, sku="SKU-DUP", purchased_price=300),
        dict(id=2, sku="SKU-DUP", purchased_price=400),
        dict(id=3, sku="SKU-ONE", purchased_price=100),
    ])
    await insert(conn, "bronze.buyers", [
        dict(id=BUYER, full_name="Buyer 1", phone="380960000001", city="Kyiv")])
    for oid, days in ORDERS:
        day = kyiv_today() - timedelta(days=days)
        await insert(conn, "silver.orders", [order_row(
            oid=oid, buyer_id=BUYER, day=day, total=1500,
            ordered_at=datetime.combine(day, time(12), KYIV))])
        await insert(conn, "bronze.order_products", [
            dict(id=oid * 1000 + 1, order_id=oid, product_id=100, name="Toner",
                 quantity=2, price_sold=500),
            dict(id=oid * 1000 + 2, order_id=oid, product_id=101, name="Serum",
                 quantity=1, price_sold=500),
        ])


@pytest_asyncio.fixture
async def admin(monkeypatch):
    async with pg_sms_seed.on_postgres(monkeypatch) as pool:
        async with pool.acquire() as conn:
            await _populate(conn)
        async with signed_in("admin") as visitor:
            yield visitor


@pytest.mark.asyncio
async def test_the_audience_costs_the_sku_once_at_its_highest_cost(admin):
    r = await admin.get(SEGMENTS, params={
        "grouping": "single", "ltv_basis": "margin", "include_customers": "true"})

    assert r.status_code == 200, r.text
    (customer,) = r.json()["customers"]
    assert customer["buyerId"] == BUYER
    assert customer["orders"] == 2
    assert customer["revenueLtv"] == 3000.0
    assert customer["marginLtv"] == 1200.0
    assert customer["ltv"] == 1200.0, "the margin basis ranks by the same figure"
    assert customer["costCoverage"] == 100.0
    assert (customer["lastOrderTotal"], customer["lastOrderItemCount"]) == (1500.0, 2)


@pytest.mark.asyncio
async def test_the_campaign_results_cost_the_sku_once(admin):
    frozen = await admin.post(CAMPAIGNS, params={
        "campaign": "two-offers", "grouping": "single", "holdout_pct": 0})
    assert frozen.status_code == 200, frozen.text
    sent_at = datetime.now(timezone.utc) - timedelta(days=SENT_DAYS_AGO)
    marked = await admin.post(f"{CAMPAIGNS}/two-offers/sent",
                              params={"sent_at": sent_at.isoformat()})
    assert marked.status_code == 200, marked.text

    r = await admin.get(f"{CAMPAIGNS}/two-offers/results", params={"window_days": 30})

    assert r.status_code == 200, r.text
    target = r.json()["overall"]["target"]
    assert (target["contacts"], target["converted"], target["orders"]) == (1, 1, 1)
    assert target["revenue"] == 1500.0
    assert target["margin"] == 600.0
