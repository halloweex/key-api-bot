"""Tests for get_sms_segments: tier assignment, eligibility filters, holdout split.

Builds a small DuckDB instance, writes `silver_orders` + `buyers` directly so
recency/frequency/LTV are exact, then asserts the segmentation output.
"""
from datetime import date, timedelta
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore


async def _make_store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "test.duckdb")
    await store.connect()
    return store


def _add_buyer(conn, buyer_id: int, phone: str | None, name: str = "Buyer") -> None:
    conn.execute(
        "INSERT INTO buyers (id, full_name, phone, city) VALUES (?, ?, ?, ?)",
        [buyer_id, f"{name} {buyer_id}", phone, "Kyiv"],
    )


# Two catalogue products: one at 40% margin, one at 80%. Costs live in
# offer_stocks and are joined through products.sku.
PRODUCT_LOW_MARGIN = 101   # sells 1000, costs 600
PRODUCT_HIGH_MARGIN = 102  # sells 1000, costs 200
PRODUCT_NO_COST = 103      # no cost on file


def _seed_catalogue(conn) -> None:
    for pid, sku in (
        (PRODUCT_LOW_MARGIN, "SKU-LOW"),
        (PRODUCT_HIGH_MARGIN, "SKU-HIGH"),
        (PRODUCT_NO_COST, "SKU-NOCOST"),
    ):
        conn.execute(
            "INSERT INTO products (id, name, sku, price) VALUES (?, ?, ?, 1000)",
            [pid, f"Product {pid}", sku],
        )
    for offer_id, (sku, cost) in enumerate((("SKU-LOW", 600), ("SKU-HIGH", 200)), start=1):
        conn.execute(
            "INSERT INTO offer_stocks (id, sku, price, purchased_price, quantity)"
            " VALUES (?, ?, 1000, ?, 10)",
            [offer_id, sku, cost],
        )


def _add_order(
    conn,
    *,
    oid: int,
    buyer_id: int,
    days_ago: int,
    total: str = "1000.00",
    is_return: bool = False,
    sales_type: str = "retail",
    is_active_source: bool = True,
    product_id: int = PRODUCT_HIGH_MARGIN,
    quantity: int = 1,
    line_price: str | None = None,
) -> None:
    """Insert an order plus its single line item.

    `line_price` defaults to `total`, so line revenue matches grand_total. Pass
    a higher value to model an order-level discount.
    """
    order_date = date.today() - timedelta(days=days_ago)
    conn.execute(
        """
        INSERT INTO silver_orders (
            id, source_id, status_id, grand_total, ordered_at, buyer_id,
            manager_id, order_date, is_return, sales_type, is_active_source,
            source_name, is_new_customer
        ) VALUES (?, 4, 1, ?, ?, ?, NULL, ?, ?, ?, ?, 'Shopify', FALSE)
        """,
        [oid, total, order_date, buyer_id, order_date, is_return, sales_type,
         is_active_source],
    )
    conn.execute(
        """
        INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [oid, oid, product_id, f"Product {product_id}", quantity,
         line_price if line_price is not None else total],
    )


async def _seed(store: DuckDBStore) -> None:
    """Seed one buyer per intended outcome."""
    async with store.connection() as conn:
        _seed_catalogue(conn)
        # 1 — VIP: LTV 30k, well inside the window
        _add_buyer(conn, 1, "+38 (096) 111-11-11")
        for i in range(3):
            _add_order(conn, oid=100 + i, buyer_id=1, days_ago=10 + i, total="10000.00")

        # 2 — CORE by order count: 2 orders, small LTV
        _add_buyer(conn, 2, "380962222222")
        _add_order(conn, oid=200, buyer_id=2, days_ago=20, total="500.00")
        _add_order(conn, oid=201, buyer_id=2, days_ago=40, total="500.00")

        # 3 — CORE by LTV: single big order
        _add_buyer(conn, 3, "380963333333")
        _add_order(conn, oid=300, buyer_id=3, days_ago=30, total="6000.00")

        # 4 — REACTIVATION: one order, inside the reactivation window
        _add_buyer(conn, 4, "380964444444")
        _add_order(conn, oid=400, buyer_id=4, days_ago=60, total="1000.00")

        # 5 — dropped: one order, past the reactivation window but inside overall
        _add_buyer(conn, 5, "380965555555")
        _add_order(conn, oid=500, buyer_id=5, days_ago=200, total="1000.00")

        # 6 — dropped: repeat buyer, but past max_recency_days
        _add_buyer(conn, 6, "380966666666")
        _add_order(conn, oid=600, buyer_id=6, days_ago=400, total="1000.00")
        _add_order(conn, oid=601, buyer_id=6, days_ago=420, total="1000.00")

        # 7 — dropped: no phone
        _add_buyer(conn, 7, None)
        _add_order(conn, oid=700, buyer_id=7, days_ago=10, total="9000.00")

        # 8 — dropped: unusable phone fragment
        _add_buyer(conn, 8, "0966")
        _add_order(conn, oid=800, buyer_id=8, days_ago=10, total="9000.00")

        # 9 — dropped: b2b, not retail
        _add_buyer(conn, 9, "380969999999")
        _add_order(conn, oid=900, buyer_id=9, days_ago=10, total="9000.00", sales_type="b2b")


def _by_id(result: dict) -> dict:
    return {c["buyerId"]: c for c in result["customers"]}


@pytest.mark.asyncio
async def test_tier_assignment_and_eligibility(tmp_path):
    store = await _make_store(tmp_path)
    await _seed(store)

    result = await store.get_sms_segments(include_customers=True, holdout_pct=0)
    customers = _by_id(result)

    assert set(customers) == {1, 2, 3, 4}, "only eligible buyers are returned"
    assert customers[1]["tier"] == "VIP"
    assert customers[2]["tier"] == "CORE"
    assert customers[3]["tier"] == "CORE"
    assert customers[4]["tier"] == "REACTIVATION"

    assert customers[1]["ltv"] == 30000.0
    assert customers[1]["orders"] == 3
    assert customers[1]["avgOrderValue"] == 10000.0

    # Phone is normalised to bare digits regardless of source formatting
    assert customers[1]["phone"] == "380961111111"

    totals = result["totals"]
    assert totals["customers"] == 4
    assert totals["target"] == 4
    assert totals["holdout"] == 0

    await store.close()


@pytest.mark.asyncio
async def test_deprecated_sources_excluded(tmp_path):
    """Gold counts revenue as `NOT is_return AND is_active_source` — match it.

    Orders from retired sources (Opencart) must not inflate LTV, order counts
    or recency, or a long-dead customer can be scored as active.
    """
    store = await _make_store(tmp_path)
    async with store.connection() as conn:
        _seed_catalogue(conn)
        _add_buyer(conn, 1, "380961111111")
        _add_order(conn, oid=1, buyer_id=1, days_ago=10, total="6000.00",
                   is_active_source=False)
        _add_order(conn, oid=2, buyer_id=1, days_ago=30, total="1000.00")

        # Only ever bought through a retired source — not a contactable customer
        _add_buyer(conn, 2, "380962222222")
        _add_order(conn, oid=3, buyer_id=2, days_ago=10, total="9000.00",
                   is_active_source=False)

    result = await store.get_sms_segments(include_customers=True, holdout_pct=0)
    customers = _by_id(result)

    assert set(customers) == {1}, "source-2 buyer has no orders on live sources"
    assert customers[1]["ltv"] == 1000.0
    assert customers[1]["orders"] == 1
    assert customers[1]["recencyDays"] == 30, "recency comes from the live order"

    await store.close()


@pytest.mark.asyncio
async def test_returns_excluded_from_ltv(tmp_path):
    store = await _make_store(tmp_path)
    async with store.connection() as conn:
        _seed_catalogue(conn)
        _add_buyer(conn, 1, "380961111111")
        _add_order(conn, oid=1, buyer_id=1, days_ago=10, total="6000.00")
        _add_order(conn, oid=2, buyer_id=1, days_ago=5, total="5000.00", is_return=True)

    result = await store.get_sms_segments(include_customers=True, holdout_pct=0)
    customer = _by_id(result)[1]

    assert customer["ltv"] == 6000.0
    assert customer["orders"] == 1, "the return must not count as a purchase"
    assert customer["tier"] == "CORE", "CORE by LTV, not VIP"

    await store.close()


@pytest.mark.asyncio
async def test_duplicate_phones_collapse_to_highest_ltv(tmp_path):
    store = await _make_store(tmp_path)
    async with store.connection() as conn:
        _seed_catalogue(conn)
        _add_buyer(conn, 1, "380961111111")
        _add_order(conn, oid=1, buyer_id=1, days_ago=10, total="1000.00")
        # Same household/number under a second buyer record
        _add_buyer(conn, 2, "+380961111111")
        _add_order(conn, oid=2, buyer_id=2, days_ago=10, total="8000.00")

    result = await store.get_sms_segments(include_customers=True, holdout_pct=0)

    assert result["totals"]["customers"] == 1, "one SMS per phone number"
    assert result["customers"][0]["buyerId"] == 2, "the higher-LTV record wins"

    await store.close()


@pytest.mark.asyncio
async def test_holdout_is_deterministic_per_campaign(tmp_path):
    store = await _make_store(tmp_path)
    async with store.connection() as conn:
        _seed_catalogue(conn)
        for i in range(1, 301):
            _add_buyer(conn, i, f"3809{i:08d}")
            _add_order(conn, oid=i, buyer_id=i, days_ago=10, total="6000.00")

    first = await store.get_sms_segments(
        include_customers=True, holdout_pct=20, campaign="aug-promo",
    )
    again = await store.get_sms_segments(
        include_customers=True, holdout_pct=20, campaign="aug-promo",
    )
    other = await store.get_sms_segments(
        include_customers=True, holdout_pct=20, campaign="sep-promo",
    )

    assignments = {c["buyerId"]: c["assignment"] for c in first["customers"]}
    assert assignments == {c["buyerId"]: c["assignment"] for c in again["customers"]}, \
        "same campaign must reproduce the same split"
    assert assignments != {c["buyerId"]: c["assignment"] for c in other["customers"]}, \
        "a new campaign must re-draw the control group"

    # 20% of 300, with slack for hash noise at this sample size
    assert 40 <= first["totals"]["holdout"] <= 80
    assert first["totals"]["target"] + first["totals"]["holdout"] == 300

    await store.close()


@pytest.mark.asyncio
async def test_holdout_zero_targets_everyone(tmp_path):
    store = await _make_store(tmp_path)
    async with store.connection() as conn:
        _seed_catalogue(conn)
        for i in range(1, 51):
            _add_buyer(conn, i, f"3809{i:08d}")
            _add_order(conn, oid=i, buyer_id=i, days_ago=10, total="6000.00")

    result = await store.get_sms_segments(holdout_pct=0)

    assert result["totals"]["holdout"] == 0
    assert result["totals"]["target"] == 50

    await store.close()


@pytest.mark.asyncio
async def test_tier_filter_and_summary_shape(tmp_path):
    store = await _make_store(tmp_path)
    await _seed(store)

    result = await store.get_sms_segments(tier="CORE", include_customers=True, holdout_pct=0)

    assert {c["tier"] for c in result["customers"]} == {"CORE"}
    assert [s["tier"] for s in result["segments"]] == ["CORE"]
    assert result["segments"][0]["total"] == 2
    assert result["segments"][0]["avgLtv"] == 3500.0  # (1000 + 6000) / 2

    await store.close()


def _funnel(result: dict) -> dict:
    return {s["stage"]: s["remaining"] for s in result["funnel"]}


@pytest.mark.asyncio
async def test_funnel_accounts_for_every_dropped_customer(tmp_path):
    """The funnel is the page's answer to "why is the list this size?".

    Each stage has to name the rule that removed people, so the seed is read
    back rule by rule rather than only at the end.
    """
    store = await _make_store(tmp_path)
    await _seed(store)

    funnel = _funnel(await store.get_sms_segments())

    # 9 buyers seeded; the b2b one never enters a retail segmentation at all.
    assert funnel["customers"] == 8
    # Buyer 6 last ordered 400 days ago.
    assert funnel["inWindow"] == 7
    # Buyer 5 is a lone order past the reactivation window — no tier fits.
    assert funnel["tiered"] == 6
    # Buyers 7 (no phone) and 8 (fragment) cannot be messaged.
    assert funnel["phone"] == 4
    assert funnel["subscribed"] == 4
    assert funnel["uniquePhone"] == 4

    await store.close()


@pytest.mark.asyncio
async def test_funnel_survives_an_empty_segmentation(tmp_path):
    """Nothing eligible is exactly when the funnel has to explain itself."""
    store = await _make_store(tmp_path)
    async with store.connection() as conn:
        _seed_catalogue(conn)
        _add_buyer(conn, 1, "380961111111")
        _add_order(conn, oid=1, buyer_id=1, days_ago=500, total="1000.00")

    result = await store.get_sms_segments()

    assert result["segments"] == []
    assert result["totals"]["customers"] == 0
    assert _funnel(result) == {
        "customers": 1, "inWindow": 0, "tiered": 0,
        "phone": 0, "subscribed": 0, "uniquePhone": 0,
    }

    await store.close()


@pytest.mark.asyncio
async def test_funnel_counts_opt_outs_separately_from_bad_phones(tmp_path):
    store = await _make_store(tmp_path)
    await _seed(store)
    await store.add_marketing_optout(buyer_id=1, phone="380961111111")

    funnel = _funnel(await store.get_sms_segments())

    assert funnel["phone"] == 4, "opting out is not a phone problem"
    assert funnel["subscribed"] == 3
    assert funnel["uniquePhone"] == 3

    await store.close()


@pytest.mark.asyncio
async def test_funnel_describes_the_base_not_the_tier_filter(tmp_path):
    """Asking for one tier narrows the roster, not the account of the base."""
    store = await _make_store(tmp_path)
    await _seed(store)

    result = await store.get_sms_segments(tier="CORE")

    assert result["totals"]["customers"] == 2
    assert _funnel(result)["uniquePhone"] == 4

    await store.close()


@pytest.mark.asyncio
async def test_customers_omitted_unless_requested(tmp_path):
    store = await _make_store(tmp_path)
    await _seed(store)

    result = await store.get_sms_segments()

    assert result["customers"] == [], "PII stays out of the default response"
    assert result["totals"]["customers"] == 4, "summary is still complete"
    assert result["truncated"] is False

    await store.close()


@pytest.mark.asyncio
async def test_limit_truncates_rows_but_not_summary(tmp_path):
    store = await _make_store(tmp_path)
    await _seed(store)

    result = await store.get_sms_segments(include_customers=True, limit=2, holdout_pct=0)

    assert len(result["customers"]) == 2
    assert result["totals"]["customers"] == 4
    assert result["truncated"] is True

    await store.close()


@pytest.mark.asyncio
async def test_last_order_is_the_most_recent_one(tmp_path):
    """The message is written around what they bought last, not first."""
    store = await _make_store(tmp_path)
    async with store.connection() as conn:
        _seed_catalogue(conn)
        _add_buyer(conn, 1, "380961111111")
        _add_order(conn, oid=10, buyer_id=1, days_ago=90, total="1000.00",
                   product_id=PRODUCT_LOW_MARGIN)
        _add_order(conn, oid=11, buyer_id=1, days_ago=15, total="4000.00",
                   product_id=PRODUCT_HIGH_MARGIN, quantity=4, line_price="1000.00")

    customer = _by_id(await store.get_sms_segments(
        include_customers=True, holdout_pct=0,
    ))[1]

    assert customer["lastOrderId"] == 11
    assert customer["lastOrderTotal"] == 4000.0
    assert customer["lastOrderItemCount"] == 1
    assert customer["lastOrderItems"] == f"Product {PRODUCT_HIGH_MARGIN}"
    assert customer["lastOrderDate"] is not None

    await store.close()


@pytest.mark.asyncio
async def test_last_order_lists_items_and_counts_the_rest(tmp_path):
    """Three names at most; anything beyond is summarised, not dropped silently."""
    store = await _make_store(tmp_path)
    async with store.connection() as conn:
        _seed_catalogue(conn)
        # Five extra catalogue lines so the last order has 5 items
        for pid in range(201, 206):
            conn.execute(
                "INSERT INTO products (id, name, sku, price) VALUES (?, ?, ?, 1000)",
                [pid, f"Item {pid}", f"SKU-{pid}"],
            )
        _add_buyer(conn, 1, "380961111111")
        _add_order(conn, oid=1, buyer_id=1, days_ago=10, total="5000.00",
                   product_id=201, quantity=5, line_price="1000.00")
        for i, pid in enumerate(range(202, 206), start=1):
            conn.execute(
                "INSERT INTO order_products (id, order_id, product_id, name, quantity,"
                " price_sold) VALUES (?, 1, ?, ?, ?, 0)",
                [100 + i, pid, f"Item {pid}", 5 - i],
            )

    customer = _by_id(await store.get_sms_segments(
        include_customers=True, holdout_pct=0,
    ))[1]

    assert customer["lastOrderItemCount"] == 5
    items = customer["lastOrderItems"]
    assert items.count(" | ") == 2, "three names, two separators"
    assert items.startswith("Product 201"), "ordered by quantity, biggest line first"
    assert items.endswith("+2 ещё"), "the omitted lines are accounted for"

    await store.close()


@pytest.mark.asyncio
async def test_long_product_names_are_truncated(tmp_path):
    """Real names run to hundreds of characters and would wreck the spreadsheet."""
    store = await _make_store(tmp_path)
    long_name = "GROWUS Damage Therapy Perfume Hand Cream, Sea Salt - " + "х" * 120
    async with store.connection() as conn:
        _seed_catalogue(conn)
        conn.execute(
            "INSERT INTO products (id, name, sku, price) VALUES (301, ?, 'SKU-LONG', 1000)",
            [long_name],
        )
        _add_buyer(conn, 1, "380961111111")
        _add_order(conn, oid=1, buyer_id=1, days_ago=10, total="1000.00", product_id=301)
        conn.execute("UPDATE order_products SET name = ? WHERE order_id = 1", [long_name])

    items = _by_id(await store.get_sms_segments(
        include_customers=True, holdout_pct=0,
    ))[1]["lastOrderItems"]

    assert len(items) == 58, "57 characters plus an ellipsis"
    assert items.endswith("…")
    assert items.startswith("GROWUS Damage Therapy")

    await store.close()


@pytest.mark.asyncio
async def test_last_order_ignores_returns_and_dead_sources(tmp_path):
    """A returned order is not what they last bought."""
    store = await _make_store(tmp_path)
    async with store.connection() as conn:
        _seed_catalogue(conn)
        _add_buyer(conn, 1, "380961111111")
        _add_order(conn, oid=1, buyer_id=1, days_ago=30, total="6000.00",
                   product_id=PRODUCT_HIGH_MARGIN)
        _add_order(conn, oid=2, buyer_id=1, days_ago=5, total="900.00", is_return=True)
        _add_order(conn, oid=3, buyer_id=1, days_ago=2, total="800.00",
                   is_active_source=False)

    customer = _by_id(await store.get_sms_segments(
        include_customers=True, holdout_pct=0,
    ))[1]

    assert customer["lastOrderId"] == 1
    assert customer["lastOrderTotal"] == 6000.0

    await store.close()


@pytest.mark.asyncio
async def test_margin_basis_reranks_customers(tmp_path):
    """The point of ltv_basis=margin: equal revenue, unequal contribution.

    Both buyers spend 8000. One buys the 80%-margin product, the other the
    40%-margin one. On revenue they are interchangeable; on margin they are not.
    """
    store = await _make_store(tmp_path)
    async with store.connection() as conn:
        _seed_catalogue(conn)
        _add_buyer(conn, 1, "380961111111")
        _add_order(conn, oid=1, buyer_id=1, days_ago=10, total="8000.00",
                   product_id=PRODUCT_HIGH_MARGIN, quantity=8, line_price="1000.00")
        _add_buyer(conn, 2, "380962222222")
        _add_order(conn, oid=2, buyer_id=2, days_ago=10, total="8000.00",
                   product_id=PRODUCT_LOW_MARGIN, quantity=8, line_price="1000.00")

    by_revenue = _by_id(await store.get_sms_segments(
        include_customers=True, holdout_pct=0, ltv_basis="revenue",
    ))
    assert by_revenue[1]["ltv"] == by_revenue[2]["ltv"] == 8000.0
    assert by_revenue[1]["tier"] == by_revenue[2]["tier"], "revenue cannot tell them apart"

    by_margin = _by_id(await store.get_sms_segments(
        include_customers=True, holdout_pct=0, ltv_basis="margin",
    ))
    assert by_margin[1]["ltv"] == 6400.0   # 8000 - 8*200
    assert by_margin[2]["ltv"] == 3200.0   # 8000 - 8*600
    assert by_margin[1]["tier"] == "VIP", "margin >= 5500"
    assert by_margin[2]["tier"] == "CORE", "margin below the VIP cut-off"

    # Both figures travel regardless of which one drives the tiering
    for result in (by_revenue, by_margin):
        assert result[1]["revenueLtv"] == 8000.0
        assert result[1]["marginLtv"] == 6400.0
        assert result[1]["marginPct"] == 80.0

    await store.close()


@pytest.mark.asyncio
async def test_order_discount_is_charged_to_margin(tmp_path):
    """grand_total carries order-level discounts; line prices do not.

    The discount must come out of margin, not out of COGS — a customer who
    only ever buys at a discount is genuinely worth less.
    """
    store = await _make_store(tmp_path)
    async with store.connection() as conn:
        _seed_catalogue(conn)
        # Lines total 10000, customer actually paid 8000 → 20% order discount
        _add_buyer(conn, 1, "380961111111")
        _add_order(conn, oid=1, buyer_id=1, days_ago=10, total="8000.00",
                   product_id=PRODUCT_HIGH_MARGIN, quantity=10, line_price="1000.00")

    customer = _by_id(await store.get_sms_segments(
        include_customers=True, holdout_pct=0, ltv_basis="margin",
    ))[1]

    assert customer["revenueLtv"] == 8000.0, "revenue follows what was actually paid"
    # COGS stays at 10 * 200 = 2000; the discount lands entirely on margin
    assert customer["marginLtv"] == 6000.0
    assert customer["marginPct"] == 75.0

    await store.close()


@pytest.mark.asyncio
async def test_uncosted_lines_reported_via_coverage(tmp_path):
    """A SKU with no cost leaves margin, stays in revenue, and shows up in coverage."""
    store = await _make_store(tmp_path)
    async with store.connection() as conn:
        _seed_catalogue(conn)
        _add_buyer(conn, 1, "380961111111")
        _add_order(conn, oid=1, buyer_id=1, days_ago=10, total="1000.00",
                   product_id=PRODUCT_HIGH_MARGIN)
        _add_order(conn, oid=2, buyer_id=1, days_ago=20, total="1000.00",
                   product_id=PRODUCT_NO_COST)

    customer = _by_id(await store.get_sms_segments(
        include_customers=True, holdout_pct=0, ltv_basis="margin",
    ))[1]

    assert customer["revenueLtv"] == 2000.0, "uncosted revenue still counts"
    assert customer["marginLtv"] == 800.0, "only the costed order contributes margin"
    assert customer["costCoverage"] == 50.0, "half the revenue has cost data"

    await store.close()


@pytest.mark.asyncio
async def test_below_cost_sales_yield_negative_margin(tmp_path):
    """Selling under cost is value destruction and must not be clipped to zero."""
    store = await _make_store(tmp_path)
    async with store.connection() as conn:
        _seed_catalogue(conn)
        _add_buyer(conn, 1, "380961111111")
        # Pays 400 for goods that cost 600
        _add_order(conn, oid=1, buyer_id=1, days_ago=10, total="400.00",
                   product_id=PRODUCT_LOW_MARGIN, quantity=1, line_price="400.00")

    customer = _by_id(await store.get_sms_segments(
        include_customers=True, holdout_pct=0, ltv_basis="margin",
    ))[1]

    assert customer["marginLtv"] == -200.0
    assert customer["revenueLtv"] == 400.0

    await store.close()


@pytest.mark.asyncio
async def test_basis_defaults_and_validation(tmp_path):
    store = await _make_store(tmp_path)
    await _seed(store)

    revenue = await store.get_sms_segments(ltv_basis="revenue")
    assert revenue["criteria"]["vipLtv"] == 10000.0
    assert revenue["criteria"]["coreLtv"] == 5000.0
    assert revenue["ltvBasis"] == "revenue"

    margin = await store.get_sms_segments(ltv_basis="margin")
    assert margin["criteria"]["vipLtv"] == 5500.0
    assert margin["criteria"]["coreLtv"] == 2750.0
    assert margin["ltvBasis"] == "margin"

    # An explicit threshold still wins over the basis default
    explicit = await store.get_sms_segments(ltv_basis="margin", vip_ltv=999)
    assert explicit["criteria"]["vipLtv"] == 999

    with pytest.raises(ValueError, match="ltv_basis"):
        await store.get_sms_segments(ltv_basis="profit")

    await store.close()


@pytest.mark.asyncio
async def test_summary_reports_both_bases(tmp_path):
    store = await _make_store(tmp_path)
    async with store.connection() as conn:
        _seed_catalogue(conn)
        _add_buyer(conn, 1, "380961111111")
        _add_order(conn, oid=1, buyer_id=1, days_ago=10, total="10000.00",
                   product_id=PRODUCT_HIGH_MARGIN, quantity=10, line_price="1000.00")

    segment = (await store.get_sms_segments(holdout_pct=0))["segments"][0]

    assert segment["tier"] == "VIP"
    assert segment["totalRevenue"] == 10000.0
    assert segment["totalMargin"] == 8000.0
    assert segment["marginPct"] == 80.0

    await store.close()


@pytest.mark.asyncio
async def test_thresholds_are_configurable(tmp_path):
    store = await _make_store(tmp_path)
    await _seed(store)

    # Lower the VIP bar: buyer 3 (LTV 6000) is promoted out of CORE
    result = await store.get_sms_segments(
        vip_ltv=6000, include_customers=True, holdout_pct=0,
    )
    assert _by_id(result)[3]["tier"] == "VIP"

    # Widen the reactivation window: buyer 5 (200 days) becomes eligible
    result = await store.get_sms_segments(
        reactivation_max_recency=250, include_customers=True, holdout_pct=0,
    )
    assert _by_id(result)[5]["tier"] == "REACTIVATION"

    # Narrow the overall window: buyer 4 (60 days) drops out
    result = await store.get_sms_segments(
        max_recency_days=35, reactivation_max_recency=35,
        include_customers=True, holdout_pct=0,
    )
    assert set(_by_id(result)) == {1, 2, 3}

    await store.close()
