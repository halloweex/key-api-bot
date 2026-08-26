"""Audience filters and grouping for SMS campaigns.

The fixed cohort — three value tiers over a 270-day window — answers "who is
worth what". It cannot answer "everyone who bought this brand since May", and
under the tier rules half of such an audience disappears silently: a one-order
buyer outside the reactivation window belongs to no tier and is dropped.

So these tests pin two things together — that a filter selects what it says,
and that `grouping="single"` stops the tier rules from filtering underneath it.
"""
from datetime import date, timedelta
from pathlib import Path

import pytest

from core.duckdb_store import DuckDBStore
from core.repositories.customers import (
    BUILTIN_AUDIENCE_PRESETS, SINGLE_GROUP_NAME, SmsAudienceFilters,
)


async def _store(tmp_path: Path) -> DuckDBStore:
    store = DuckDBStore(db_path=tmp_path / "filters.duckdb")
    await store.connect()
    return store


def _catalogue(conn) -> None:
    conn.execute(
        "INSERT INTO categories (id, name, parent_id) VALUES "
        "(10, 'Face care', NULL), (11, 'Masks', 10), (20, 'Hair', NULL)"
    )
    for pid, sku, brand, category in (
        (101, "SKU-A", "Medi-Peel", 11),
        (102, "SKU-B", "Anua", 20),
    ):
        conn.execute(
            "INSERT INTO products (id, name, sku, brand, category_id, price)"
            " VALUES (?, ?, ?, ?, ?, 1000)",
            [pid, f"Product {pid}", sku, brand, category],
        )


def _buyer(conn, buyer_id: int, phone: str, city: str = "Kyiv") -> None:
    conn.execute(
        "INSERT INTO buyers (id, full_name, phone, city) VALUES (?, ?, ?, ?)",
        [buyer_id, f"Buyer {buyer_id}", phone, city],
    )


def _order(
    conn, *, oid: int, buyer_id: int, days_ago: int, total: str = "1000.00",
    product_id: int = 101, source_id: int = 4, promocode: str | None = None,
) -> None:
    when = date.today() - timedelta(days=days_ago)
    conn.execute(
        """
        INSERT INTO silver_orders (
            id, source_id, status_id, grand_total, ordered_at, buyer_id,
            manager_id, order_date, is_return, sales_type, is_active_source,
            source_name, is_new_customer, promocode
        ) VALUES (?, ?, 1, ?, ?, ?, NULL, ?, FALSE, 'retail', TRUE,
                  'Shopify', FALSE, ?)
        """,
        [oid, source_id, total, when, buyer_id, when, promocode],
    )
    conn.execute(
        "INSERT INTO order_products (id, order_id, product_id, name, quantity, price_sold)"
        " VALUES (?, ?, ?, ?, 1, ?)",
        [oid, oid, product_id, f"Product {product_id}", total],
    )


async def _seed(store: DuckDBStore) -> None:
    """Four buyers, each the sole answer to one question.

    Values are chosen so nobody qualifies for VIP or CORE: everyone here is a
    one-order buyer, which is exactly the population the tier rules discard.
    """
    async with store.connection() as conn:
        _catalogue(conn)
        # 1 — Medi-Peel (a Masks product), Kyiv, recent, promocode
        _buyer(conn, 1, "380961111111", city="Kyiv")
        _order(conn, oid=1, buyer_id=1, days_ago=20, product_id=101,
               promocode="KS-AUG")
        # 2 — Anua (Hair), Lviv, recent
        _buyer(conn, 2, "380962222222", city="Lviv")
        _order(conn, oid=2, buyer_id=2, days_ago=30, product_id=102)
        # 3 — Medi-Peel, but long ago: outside every "recently" window
        _buyer(conn, 3, "380963333333", city="Kyiv")
        _order(conn, oid=3, buyer_id=3, days_ago=200, product_id=101)
        # 4 — big single order, Instagram, no promocode
        _buyer(conn, 4, "380964444444", city="Kyiv")
        _order(conn, oid=4, buyer_id=4, days_ago=15, total="8000.00",
               product_id=102, source_id=1)


async def _ids(store, **kwargs) -> set:
    data = await store.get_sms_segments(
        grouping=kwargs.pop("grouping", "single"),
        include_customers=True,
        max_recency_days=kwargs.pop("max_recency_days", 270),
        filters=SmsAudienceFilters(**kwargs) if kwargs else None,
    )
    return {c["buyerId"] for c in data["customers"]}


@pytest.mark.asyncio
async def test_single_grouping_keeps_buyers_no_tier_would_hold(tmp_path):
    """The reason grouping exists at all."""
    store = await _store(tmp_path)
    try:
        await _seed(store)

        rfm = await store.get_sms_segments(grouping="rfm", include_customers=True)
        single = await store.get_sms_segments(grouping="single", include_customers=True)

        # Buyer 3 last ordered 200 days ago with one order: inside the 270-day
        # window, outside the 120-day reactivation window, so no tier holds it.
        assert 3 not in {c["buyerId"] for c in rfm["customers"]}
        assert 3 in {c["buyerId"] for c in single["customers"]}
        assert {s["tier"] for s in single["segments"]} == {SINGLE_GROUP_NAME}
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_no_filters_changes_nothing(tmp_path):
    """An empty filter set must select what the rules alone selected."""
    store = await _store(tmp_path)
    try:
        await _seed(store)

        before = await store.get_sms_segments(include_customers=True)
        after = await store.get_sms_segments(
            include_customers=True, filters=SmsAudienceFilters(),
        )
        assert before["segments"] == after["segments"]
        assert before["totals"] == after["totals"]
    finally:
        await store.close()


@pytest.mark.asyncio
class TestFilters:
    async def test_brand(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await _seed(store)
            assert await _ids(store, brands=["Medi-Peel"]) == {1, 3}
        finally:
            await store.close()

    async def test_category_takes_the_whole_branch(self, tmp_path):
        """Picking a parent category must not return an empty list."""
        store = await _store(tmp_path)
        try:
            await _seed(store)
            # 10 is 'Face care'; the products hang off its child 'Masks' (11).
            assert await _ids(store, category_ids=[10]) == {1, 3}
            assert await _ids(store, category_ids=[11]) == {1, 3}
            assert await _ids(store, category_ids=[20]) == {2, 4}
        finally:
            await store.close()

    async def test_bought_within_days_narrows_the_content_rule(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await _seed(store)
            assert await _ids(store, brands=["Medi-Peel"]) == {1, 3}
            assert await _ids(
                store, brands=["Medi-Peel"], bought_within_days=90,
            ) == {1}
        finally:
            await store.close()

    async def test_city_is_case_insensitive(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await _seed(store)
            assert await _ids(store, cities=["kyiv"]) == {1, 3, 4}
            assert await _ids(store, cities=["Lviv"]) == {2}
        finally:
            await store.close()

    async def test_source(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await _seed(store)
            assert await _ids(store, source_ids=[1]) == {4}
        finally:
            await store.close()

    async def test_promocode(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await _seed(store)
            assert await _ids(store, promocode="ks-aug") == {1}
        finally:
            await store.close()

    async def test_recency_window_has_two_edges(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await _seed(store)
            assert await _ids(store, recency_max_days=25) == {1, 4}
            assert await _ids(store, recency_min_days=100) == {3}
        finally:
            await store.close()

    async def test_ltv_and_aov(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await _seed(store)
            # Buyer 4 is the only one above 5 000 — on either basis, since a
            # costless product leaves margin at zero and revenue is the default.
            assert await _ids(store, ltv_min=5000) == {4}
            assert await _ids(store, aov_min=5000) == {4}
            assert await _ids(store, ltv_max=1500) == {1, 2, 3}
        finally:
            await store.close()

    async def test_orders_count(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await _seed(store)
            assert await _ids(store, orders_min=2) == set()
            assert await _ids(store, orders_max=1) == {1, 2, 3, 4}
        finally:
            await store.close()

    async def test_first_order_date(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await _seed(store)
            assert await _ids(
                store, first_order_from=date.today() - timedelta(days=25),
            ) == {1, 4}
        finally:
            await store.close()

    async def test_filters_combine_with_and(self, tmp_path):
        store = await _store(tmp_path)
        try:
            await _seed(store)
            assert await _ids(store, brands=["Medi-Peel"], cities=["Kyiv"]) == {1, 3}
            assert await _ids(
                store, brands=["Medi-Peel"], cities=["Lviv"],
            ) == set()
        finally:
            await store.close()


@pytest.mark.asyncio
async def test_funnel_reports_what_the_filters_removed(tmp_path):
    """A filtered audience that comes back small has to say where it went."""
    store = await _store(tmp_path)
    try:
        await _seed(store)
        data = await store.get_sms_segments(
            grouping="single", filters=SmsAudienceFilters(brands=["Medi-Peel"]),
        )
        stages = {s["stage"]: s["remaining"] for s in data["funnel"]}
        assert stages["inWindow"] == 4
        assert stages["filtered"] == 2
        assert stages["uniquePhone"] == 2
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_criteria_echoes_the_filters(tmp_path):
    """Frozen with the campaign, so it has to survive the round trip."""
    store = await _store(tmp_path)
    try:
        await _seed(store)
        data = await store.get_sms_segments(
            grouping="single",
            filters=SmsAudienceFilters(brands=["Anua"], cities=["Lviv"], ltv_min=100),
        )
        assert data["criteria"]["filters"] == {
            "ltv_min": 100, "cities": ["Lviv"], "brands": ["Anua"],
        }
        assert data["criteria"]["grouping"] == "single"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_grouping_is_validated(tmp_path):
    store = await _store(tmp_path)
    try:
        with pytest.raises(ValueError, match="grouping"):
            await store.get_sms_segments(grouping="cohorts")
    finally:
        await store.close()


# ─── Saved audiences ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_presets_round_trip(tmp_path):
    store = await _store(tmp_path)
    try:
        criteria = {"grouping": "single", "filters": {"brands": ["Anua"]}}
        await store.save_sms_audience_preset("Anua buyers", criteria, created_by=7)

        saved = {p["name"]: p for p in await store.list_sms_audience_presets()}
        assert saved["Anua buyers"]["criteria"] == criteria
        assert saved["Anua buyers"]["createdBy"] == 7
        assert saved["Anua buyers"]["builtin"] is False

        # Built-ins are always present and always flagged.
        for name in BUILTIN_AUDIENCE_PRESETS:
            assert saved[name]["builtin"] is True
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_saving_the_same_name_replaces_it(tmp_path):
    store = await _store(tmp_path)
    try:
        await store.save_sms_audience_preset("x", {"a": 1})
        await store.save_sms_audience_preset("x", {"a": 2})

        rows = [p for p in await store.list_sms_audience_presets() if p["name"] == "x"]
        assert len(rows) == 1
        assert rows[0]["criteria"] == {"a": 2}
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_builtins_cannot_be_replaced_or_deleted(tmp_path):
    """The reference cohort has to keep meaning what past campaigns meant."""
    store = await _store(tmp_path)
    name = next(iter(BUILTIN_AUDIENCE_PRESETS))
    try:
        with pytest.raises(ValueError, match="built-in"):
            await store.save_sms_audience_preset(name, {})
        with pytest.raises(ValueError, match="built-in"):
            await store.delete_sms_audience_preset(name)
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_deleting_reports_whether_anything_went(tmp_path):
    store = await _store(tmp_path)
    try:
        await store.save_sms_audience_preset("temp", {})
        assert await store.delete_sms_audience_preset("temp") is True
        assert await store.delete_sms_audience_preset("temp") is False
    finally:
        await store.close()


# ─── The value level filters, whether or not it splits ────────────────────

@pytest.mark.asyncio
async def test_level_filters_under_one_arm(tmp_path):
    """"Send to VIP only" is a filter, not a way of splitting the result.

    Tying the two together is what made three tier cards read as three
    audiences: the level says who is messaged, the grouping says what the
    result is measured on, and they are separate decisions.
    """
    store = await _store(tmp_path)
    try:
        async with store.connection() as conn:
            _catalogue(conn)
            # One valuable repeat buyer, one modest single-order buyer.
            _buyer(conn, 1, "380961111111")
            for oid in (11, 12):
                _order(conn, oid=oid, buyer_id=1, days_ago=20, total="9000.00")
            _buyer(conn, 2, "380962222222")
            _order(conn, oid=21, buyer_id=2, days_ago=20, total="500.00")

        both = await store.get_sms_segments(
            grouping="single", ltv_basis="revenue", include_customers=True,
        )
        assert {c["buyerId"] for c in both["customers"]} == {1, 2}
        assert {s["tier"] for s in both["segments"]} == {SINGLE_GROUP_NAME}

        # Same single arm, but only the VIPs are in it.
        vip_only = await store.get_sms_segments(
            grouping="single", ltv_basis="revenue", tier=["VIP"],
            include_customers=True,
        )
        assert {c["buyerId"] for c in vip_only["customers"]} == {1}
        assert {s["tier"] for s in vip_only["segments"]} == {SINGLE_GROUP_NAME}

        # And under three arms the level is both filter and arm.
        split = await store.get_sms_segments(
            grouping="rfm", ltv_basis="revenue", tier=["VIP"],
            include_customers=True,
        )
        assert {s["tier"] for s in split["segments"]} == {"VIP"}
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_one_arm_still_keeps_buyers_of_no_level(tmp_path):
    """Without a level filter, "one arm" must not drop the levelless."""
    store = await _store(tmp_path)
    try:
        await _seed(store)

        single = await store.get_sms_segments(grouping="single", include_customers=True)
        rfm = await store.get_sms_segments(grouping="rfm", include_customers=True)

        # Buyer 3 last ordered 200 days ago with one order: no level holds it.
        assert 3 in {c["buyerId"] for c in single["customers"]}
        assert 3 not in {c["buyerId"] for c in rfm["customers"]}
    finally:
        await store.close()
