"""The `/inventory` view layer: one body, two engines.

Eleven views, eight repository methods, and until this change the bodies lived
inside `core/duckdb_store.py` as one DuckDB-only string. The same contract the
Silver projection and `silver.order_lines` already have applies here: the text
is written once and the table names are the only thing allowed to differ.

The test that matters is the one undoing every substitution and demanding the
two renderings are the same string — that is what proves there is no *further*
divergence, the thing an `if postgres:` in the body could hide indefinitely.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from core.sql_dialect import DUCKDB, inventory_view_selects
from tests.sql_helper import strip_comments_and_literals as _strip_comments_and_literals


class TestCreationOrder:
    """The views reference each other, so the order is part of the contract."""

    def test_every_view_is_created_after_the_views_it_reads(self):
        created: set[str] = set()
        for name, sql in inventory_view_selects(DUCKDB):
            referenced = set(re.findall(r"\bv_[a-z0-9_]+\b",
                                        _strip_comments_and_literals(sql)))
            missing = referenced - created
            assert not missing, f"{name} reads {sorted(missing)} before they exist"
            created.add(name)

    def test_the_root_comes_first_and_the_deepest_last(self):
        names = [n for n, _ in inventory_view_selects(DUCKDB)]
        assert names[0] == "v_sku_analysis"
        assert names.index("v_category_velocity") < names.index("v_sku_status")
        assert names.index("v_abc_classification") < names.index("v_sku_dead_stock_v2")


# `TestTheStoreDoesNotKeepItsOwnCopy` stood here until step 14's S14-18: the
# DuckDB store's `_create_inventory_views` rendered the views from the shared
# body and held no SQL of its own. The store's schema went with its DuckDB
# half; Postgres renders the same body (revision 0014).


class TestTheConsumersAreCovered:
    """The eight methods read views by name. A view dropped from the body
    would leave a method querying something that no longer exists, and the
    only test that would notice is one that reads the methods."""

    def test_every_view_the_repository_names_is_in_the_body(self):
        source = _strip_comments_and_literals(
            Path("core/repositories/inventory.py").read_text(encoding="utf-8")
        )
        wanted = set(re.findall(r"\bv_[a-z0-9_]+\b", source))
        have = {n for n, _ in inventory_view_selects(DUCKDB)}
        assert wanted <= have, f"repository reads views that do not exist: {wanted - have}"

    @pytest.mark.parametrize("view", [
        "v_sku_analysis", "v_category_velocity", "v_sku_status",
        "v_inventory_summary", "v_aging_buckets", "v_sku_sell_through",
        "v_abc_classification", "v_abc_summary", "v_recommended_actions",
        "v_restock_alerts", "v_sku_dead_stock_v2",
    ])
    def test_the_eleven_are_all_present(self, view):
        assert view in {n for n, _ in inventory_view_selects(DUCKDB)}

    # `core.pg.REQUIRED_REVISION` pins the head migration, and the assertion
    # that it moved lives in `tests/unit/test_order_versions.py` — one home,
    # and it is deliberately a speed bump rather than a convenience.
