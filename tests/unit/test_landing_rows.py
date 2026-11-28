"""One reading of a KeyCRM payload, and proof it reads the same as before.

Step 05 hands landing rows to two stores. If each store reads the payload for
itself, the rule has two homes and this codebase's record for two homes of a
rule staying in step is under a day (#101). So the reading moved into
`core.landing_rows`, pure.

The tests that matter here are not the happy paths — they are the awkward
corners, because a refactor that quietly "fixes" one of them changes production
numbers while looking like tidying. Brand in particular: it decides which
bucket a product's revenue lands in, and the owner presents those buckets to
suppliers. Each corner is stated with the brand it reads as.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.landing_rows import (
    BRAND_FIELD_NAME,
    BRAND_FIELD_UUID,
    brand_of,
    category_row,
    category_rows,
    product_row,
    product_rows,
)


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

    def test_a_field_that_is_not_the_brand_is_passed_over(self):
        p = {"custom_fields": [
            {"uuid": "CT_OTHER", "value": ["x"]},
            {"name": BRAND_FIELD_NAME, "value": ["Abib"]},
        ]}
        assert brand_of(p) == "Abib"

    @pytest.mark.parametrize("value", [None, "NEOGEN"])
    def test_a_value_that_is_not_a_list_is_none(self, value):
        """KeyCRM sends the field's values as a list. A null, or a bare
        string, is no brand, never the string itself."""
        p = {"custom_fields": [{"uuid": BRAND_FIELD_UUID, "value": value}]}
        assert brand_of(p) is None


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


class TestTheStoreReadsThroughIt:
    def test_no_second_reading_is_left_in_the_store(self):
        """The point of the move. A `CT_1001` here again means the rule has
        two homes, which is where #101 came from."""
        import inspect
        from core import store as core_store

        src = inspect.getsource(core_store)
        assert "CT_1001" not in src
        assert 'min_price' not in src

    @pytest.mark.asyncio
    async def test_an_empty_list_still_writes_nothing(self, tmp_path):
        from core.store import AnalyticsStore

        store = AnalyticsStore()
        assert await store.upsert_categories([]) == 0
        assert await store.upsert_products([]) == 0
