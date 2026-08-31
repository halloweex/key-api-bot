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

import ast
import re
from pathlib import Path

import pytest

from core.sql_dialect import DUCKDB, POSTGRES, inventory_view_selects

# The names the two dialects put in front of the same body.
_SUBSTITUTIONS = (
    ("app.sku_inventory_status", "sku_inventory_status"),
    ("bronze.categories", "categories"),
    ("bronze.offer_stocks", "offer_stocks"),
    ("silver.order_lines", "silver_order_lines"),
    ("gold.v_", "v_"),
)


def _undo_postgres_names(sql: str) -> str:
    for pg_name, duckdb_name in _SUBSTITUTIONS:
        sql = sql.replace(pg_name, duckdb_name)
    return sql


def _strip_comments_and_literals(sql: str) -> str:
    """SQL with `--` comments and `'…'` literals blanked out.

    Prose is not structure. A comment naming a table would make a scan for
    that table's name report a dependency the query does not have, and the
    reverse mistake — a scanner tripping over an apostrophe in a comment —
    has already cost this repository two debugging rounds
    (`core.pg_sms_read.numbered`).
    """
    out: list[str] = []
    in_comment = in_literal = False
    i = 0
    while i < len(sql):
        char = sql[i]
        if in_comment:
            if char == "\n":
                in_comment = False
                out.append(char)
        elif in_literal:
            if char == "'":
                in_literal = False
        elif sql[i:i + 2] == "--":
            in_comment = True
        elif char == "'":
            in_literal = True
        else:
            out.append(char)
        i += 1
    return "".join(out)


class TestOneBodyTwoEngines:
    def test_the_two_renderings_differ_only_in_table_names(self):
        duckdb_views = inventory_view_selects(DUCKDB)
        postgres_views = inventory_view_selects(POSTGRES)

        assert [n for n, _ in duckdb_views] == [
            _undo_postgres_names(n) for n, _ in postgres_views
        ]
        for (dk_name, dk_sql), (_, pg_sql) in zip(duckdb_views, postgres_views):
            assert _undo_postgres_names(pg_sql) == dk_sql, dk_name

    def test_duckdb_renders_no_schema_prefix(self):
        assert DUCKDB.inventory_views == ""
        assert all(n.startswith("v_") for n, _ in inventory_view_selects(DUCKDB))

    def test_postgres_puts_them_in_gold(self):
        # `app` is for what nothing can decide again; every one of these is
        # derived and a migration puts them all back.
        assert POSTGRES.inventory_views == "gold."
        assert all(n.startswith("gold.v_") for n, _ in inventory_view_selects(POSTGRES))


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


class TestTheGoldProductsDependencyIsGone:
    """`gold_daily_products` has no Postgres counterpart, and the views no
    longer need one: they asked it for a per-product rollup the order-lines
    level carries in both engines. Measured on the production backup, the two
    formulations return identical rows for all eleven views."""

    def test_no_view_reads_gold_daily_products(self):
        for name, sql in inventory_view_selects(DUCKDB):
            assert "gold_daily_products" not in _strip_comments_and_literals(sql), name

    def test_the_rollups_reapply_golds_own_predicate(self):
        # Gold materialised those rows under `NOT is_return AND
        # is_active_source`. Reading the line level without re-applying it
        # would silently add returns and Opencart back into every velocity
        # figure on the tab.
        for name, sql in inventory_view_selects(DUCKDB):
            body = _strip_comments_and_literals(sql)
            for match in re.finditer(r"FROM silver_order_lines\b(.*?)GROUP BY", body, re.S):
                assert "NOT is_return" in match.group(1), name
                assert "is_active_source" in match.group(1), name

    def test_every_rollup_reads_the_line_level_and_nothing_else(self):
        sources = set()
        for _, sql in inventory_view_selects(DUCKDB):
            body = _strip_comments_and_literals(sql)
            sources |= set(re.findall(r"(?:FROM|JOIN)\s+(?!\()([a-z_][a-z0-9_]*)", body))
        cte_names = {"base", "cost_ratio", "product_revenue", "ranked",
                     "sales_30", "sales_90"}
        view_names = {n for n, _ in inventory_view_selects(DUCKDB)}
        assert sources - cte_names - view_names == {
            "sku_inventory_status", "categories", "offer_stocks", "silver_order_lines",
        }


class TestTheStoreDoesNotKeepItsOwnCopy:
    """The bodies moved out of `core/duckdb_store.py`; a copy left behind is
    the failure mode charter rule 1 names, and it took under a day the last
    time (#101 updated one Silver projection and not the other)."""

    def test_create_inventory_views_holds_no_sql_of_its_own(self):
        source = Path("core/duckdb_store.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        func = next(
            node for node in ast.walk(tree)
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name == "_create_inventory_views"
        )
        literals = [
            node.value for node in ast.walk(func)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        ]
        assert not any("CREATE OR REPLACE VIEW v_" in text for text in literals)
        assert not any("gold_daily_products" in text for text in literals)

    def test_the_store_renders_from_the_shared_body(self):
        source = Path("core/duckdb_store.py").read_text(encoding="utf-8")
        assert "inventory_view_selects as _inventory_view_selects" in source


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
