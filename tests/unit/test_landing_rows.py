"""One reading of a KeyCRM payload, and proof it reads the same as before.

Step 05 hands landing rows to two stores. If each store reads the payload for
itself, the rule has two homes and this codebase's record for two homes of a
rule staying in step is under a day (#101). So the reading moved into
`core.landing_rows`, pure.

The tests that matter here are not the happy paths — they are the awkward
corners, because a refactor that quietly "fixes" one of them changes production
numbers while looking like tidying. Brand in particular: it decides which
bucket a product's revenue lands in, and the owner presents those buckets to
suppliers.

`TestNothingChanged` replays each corner through the pre-refactor expression
written out by hand, so the two are compared rather than asserted from memory.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.landing_rows import (
    BRAND_FIELD_NAME,
    BRAND_FIELD_UUID,
    CATEGORY_COLUMNS,
    PRODUCT_COLUMNS,
    brand_of,
    category_row,
    category_rows,
    product_row,
    product_rows,
)


def _old_brand(prod_data):
    """The expression this replaced, copied verbatim from duckdb_store.py."""
    brand = None
    for cf in prod_data.get("custom_fields", []):
        if cf.get("uuid") == "CT_1001" or cf.get("name") == "Brand":
            values = cf.get("value", [])
            if values and isinstance(values, list):
                brand = values[0]
            break
    return brand


class TestBrand:
    def test_it_is_found_by_uuid(self):
        p = {"custom_fields": [{"uuid": BRAND_FIELD_UUID, "value": ["NEOGEN"]}]}
        assert brand_of(p) == "NEOGEN"

    def test_it_is_found_by_name_when_the_uuid_differs(self):
        """A KeyCRM account can rename a field but not re-issue its uuid, and
        this codebase has met both spellings."""
        p = {"custom_fields": [{"uuid": "CT_9999", "name": BRAND_FIELD_NAME,
                                "value": ["Abib"]}]}
        assert brand_of(p) == "Abib"

    def test_absent_field_is_none(self):
        assert brand_of({"custom_fields": []}) is None
        assert brand_of({}) is None

    def test_null_custom_fields_is_none(self):
        """KeyCRM sends `null`, not `[]`, for a product with no custom fields.
        `.get("custom_fields", [])` returns None for that and the old loop
        raised; `or []` is the one place this is more forgiving, and it can
        only turn a crash into a None."""
        assert brand_of({"custom_fields": None}) is None

    def test_an_empty_value_list_is_none(self):
        p = {"custom_fields": [{"uuid": BRAND_FIELD_UUID, "value": []}]}
        assert brand_of(p) is None

    def test_only_the_first_value_survives(self):
        """A multi-valued field loses everything after the first entry, and did
        so before this function existed. Changing it would move revenue between
        brand buckets — a decision about the catalogue, not about the code."""
        p = {"custom_fields": [{"uuid": BRAND_FIELD_UUID,
                                "value": ["NEOGEN", "ANUA"]}]}
        assert brand_of(p) == "NEOGEN"

    def test_the_first_matching_field_wins_and_search_stops(self):
        p = {"custom_fields": [
            {"uuid": BRAND_FIELD_UUID, "value": []},
            {"name": BRAND_FIELD_NAME, "value": ["Abib"]},
        ]}
        assert brand_of(p) is None, "the first match ends the search, empty or not"


class TestCategory:
    def test_a_full_payload(self):
        r = category_row({"id": 7, "name": "Care", "parent_id": 1})
        assert r == (7, "Care", 1)

    def test_a_missing_name_becomes_unknown(self):
        """Not an accident: a nameless category becomes one named Unknown
        rather than a failed sync."""
        assert category_row({"id": 7}).name == "Unknown"

    def test_an_explicit_null_name_is_kept(self):
        """`.get(k, default)` returns None for an explicit null — the default
        only applies to an absent key. Preserved, because the database is
        where that gets refused."""
        assert category_row({"id": 7, "name": None}).name is None

    def test_a_missing_id_is_not_rejected_here(self):
        """Validation belongs to the transaction that has to roll back."""
        assert category_row({"name": "Care"}).id is None

    def test_columns_match_the_row(self):
        assert CATEGORY_COLUMNS == ("id", "name", "parent_id")


class TestProduct:
    def test_a_full_payload(self):
        r = product_row({
            "id": 100, "name": "Serum", "category_id": 2, "sku": "SKU-A",
            "min_price": 500.0, "price": 700.0,
            "custom_fields": [{"uuid": BRAND_FIELD_UUID, "value": ["NEOGEN"]}],
        })
        assert r == (100, "Serum", 2, "NEOGEN", "SKU-A", 500.0)

    def test_price_falls_back_to_price_when_min_price_is_absent(self):
        assert product_row({"id": 1, "price": 700.0}).price == 700.0

    def test_a_min_price_of_zero_falls_through_to_price(self):
        """`or`, not `if is None`. Whether that is right is a question about
        KeyCRM's catalogue; it is not this module's to answer."""
        assert product_row({"id": 1, "min_price": 0, "price": 700.0}).price == 700.0

    def test_no_price_at_all_is_none(self):
        assert product_row({"id": 1}).price is None

    def test_columns_match_the_row(self):
        assert PRODUCT_COLUMNS == (
            "id", "name", "category_id", "brand", "sku", "price",
        )

    def test_bookkeeping_is_not_a_column(self):
        """`synced_at` and friends differ between two correct stores by
        construction. In a shared row they would make every row look like a
        discrepancy to the reconciliation comparing the two."""
        for col in ("synced_at", "first_seen_at", "update_count"):
            assert col not in PRODUCT_COLUMNS
            assert col not in CATEGORY_COLUMNS


class TestNothingChanged:
    """The old expression, written out by hand, against the new function."""

    @pytest.mark.parametrize("payload", [
        {},
        {"custom_fields": []},
        {"custom_fields": [{"uuid": "CT_1001", "value": ["NEOGEN"]}]},
        {"custom_fields": [{"uuid": "CT_1001", "value": []}]},
        {"custom_fields": [{"uuid": "CT_1001", "value": None}]},
        {"custom_fields": [{"uuid": "CT_1001", "value": "NEOGEN"}]},
        {"custom_fields": [{"name": "Brand", "value": ["Abib"]}]},
        {"custom_fields": [{"uuid": "CT_OTHER", "value": ["x"]},
                           {"name": "Brand", "value": ["Abib"]}]},
        {"custom_fields": [{"uuid": "CT_1001", "value": ["A", "B"]}]},
    ])
    def test_brand_agrees_with_the_expression_it_replaced(self, payload):
        assert brand_of(payload) == _old_brand(payload)

    @pytest.mark.parametrize("payload", [
        {"id": 1, "name": "X", "category_id": 2, "sku": "S", "min_price": 5, "price": 9},
        {"id": 1, "min_price": 0, "price": 9},
        {"id": 1, "price": 9},
        {"id": 1},
        {},
    ])
    def test_product_agrees_with_the_expressions_it_replaced(self, payload):
        assert product_row(payload) == (
            payload.get("id"),
            payload.get("name", "Unknown"),
            payload.get("category_id"),
            _old_brand(payload),
            payload.get("sku"),
            payload.get("min_price") or payload.get("price"),
        )

    @pytest.mark.parametrize("payload", [
        {"id": 1, "name": "Care", "parent_id": 2},
        {"id": 1},
        {"id": 1, "name": None},
        {},
    ])
    def test_category_agrees_with_the_expressions_it_replaced(self, payload):
        assert category_row(payload) == (
            payload.get("id"),
            payload.get("name", "Unknown"),
            payload.get("parent_id"),
        )


class TestTheStoreReadsThroughIt:
    def test_no_second_reading_is_left_in_the_store(self):
        """The point of the move. A `CT_1001` here again means the rule has
        two homes, which is where #101 came from."""
        import inspect
        from core import duckdb_store

        src = inspect.getsource(duckdb_store)
        assert "CT_1001" not in src
        assert 'min_price' not in src

    @pytest.mark.asyncio
    async def test_a_real_upsert_still_writes_the_same_row(self, tmp_path):
        from core.duckdb_store import DuckDBStore

        store = DuckDBStore(db_path=tmp_path / "landing.duckdb")
        await store.connect()
        try:
            await store.upsert_categories([{"id": 1, "name": "Care", "parent_id": None}])
            await store.upsert_products([{
                "id": 100, "name": "Serum", "category_id": 1, "sku": "SKU-A",
                "min_price": 0, "price": 700.0,
                "custom_fields": [{"uuid": BRAND_FIELD_UUID, "value": ["NEOGEN"]}],
            }])
            async with store.connection() as conn:
                cat = conn.execute(
                    "SELECT id, name, parent_id FROM categories"
                ).fetchone()
                prod = conn.execute(
                    "SELECT id, name, category_id, brand, sku, price FROM products"
                ).fetchone()
            assert cat == (1, "Care", None)
            assert prod == (100, "Serum", 1, "NEOGEN", "SKU-A", 700.0)
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_an_empty_list_still_writes_nothing(self, tmp_path):
        from core.duckdb_store import DuckDBStore

        store = DuckDBStore(db_path=tmp_path / "empty.duckdb")
        await store.connect()
        try:
            assert await store.upsert_categories([]) == 0
            assert await store.upsert_products([]) == 0
        finally:
            await store.close()
