"""The product-intelligence statements.

The numbers are `tests/integration/test_products_intel_two_engines.py`'s job,
against a real Postgres, which calls every method for real — `/margin` taught
that a method which parses can still be one PostgreSQL cannot execute, and
that a fallback turns that into a silent success. That the router asks
Postgres and nothing else since step 14's S14-05 is
`tests/unit/test_read_routers_postgres_only.py`'s. Here: the holes and the ties.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

from core.sql_dialect import POSTGRES, render_tables

REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "products_intel.py"


class TestTheRoutedBodies:
    def _statements(self):
        src = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "_intel_run"):
                continue
            for part in ast.walk(node):
                if isinstance(part, ast.JoinedStr):
                    text = "".join(v.value for v in part.values
                                   if isinstance(v, ast.Constant)
                                   and isinstance(v.value, str))
                    if "SELECT" in text.upper():
                        yield node.lineno, text

    def test_the_scan_finds_them(self):
        assert len(list(self._statements())) >= 6

    def test_every_hole_survived_the_f_string(self):
        """A single brace inside an f-string is an expression Python evaluates,
        not a hole — it parses and raises `NameError` on the first call."""
        for lineno, text in self._statements():
            assert "{order_lines}" in text, f"line {lineno}: no table hole"

    def test_the_rendering_leaves_no_hole(self):
        for lineno, text in self._statements():
            rendered = render_tables(text, POSTGRES)
            assert "{" not in rendered and "}" not in rendered, (
                f"line {lineno} unrendered for {POSTGRES.name}")

    def test_the_duckdb_only_gold_is_gone(self):
        """`gold_daily_products` exists only in DuckDB. The line level
        reproduces it to the kopeck under Gold's predicate, which is why
        momentum reads that instead."""
        body = "\n".join(l.split("--")[0]
                         for l in REPOSITORY.read_text(encoding="utf-8").splitlines())
        assert "gold_daily_products" not in body

    def test_momentum_keeps_golds_grain(self):
        """Gold's rows carry the name *as sold*, so a product sold under two
        names is two rows there. Dropping `product_name` from the key would
        merge them and change a number the page has always shown."""
        text = next(t for _l, t in self._statements() if "current_period" in t)
        assert "GROUP BY l.product_id, l.product_name" in text
        assert "NOT l.is_return AND l.is_active_source" in text

    # Columns that cannot tie within their own result, so a single-key sort on
    # one of them is already deterministic. `sort_order` is a CASE over six
    # buckets and is grouped on, so there is exactly one row per value — the
    # same exemption `test_pg_inventory_read.UNIQUE_KEYS` makes, and worth
    # stating rather than counting commas.
    UNIQUE_KEYS = frozenset({"sort_order"})

    def test_every_ordered_result_ends_on_a_key_that_cannot_tie(self):
        """Four of these carry a LIMIT, where a tie means different rows."""
        offenders = []
        for lineno, text in self._statements():
            body = "\n".join(l.split("--")[0] for l in text.splitlines())
            upper = body.upper()
            if "ORDER BY" not in upper:
                continue
            tail = body[upper.rindex("ORDER BY") + 8:].split("LIMIT")[0]
            parts = [c.strip() for c in tail.split(",") if c.strip()]
            last = parts[-1].split()[0]
            if len(parts) < 2 and last not in self.UNIQUE_KEYS:
                offenders.append((lineno, tail.strip()))
        assert not offenders, f"single-key ORDER BY, so a tie is the planner's: {offenders}"

    def test_no_having_leans_on_a_select_alias(self):
        """DuckDB resolves a select alias in HAVING; PostgreSQL does not.

        The lenient spelling renders fine and fails only on the other engine —
        `column "co_occurrence" does not exist`. It bit the marketing brand
        query first and three of these next, which is twice too many for a
        rule that a scan can hold.
        """
        offenders = []
        for lineno, text in self._statements():
            aliases = set(re.findall(r"\bAS\s+([a-z_][a-z0-9_]*)", text, re.I))
            for having in re.findall(r"HAVING\s+([^\n]+)", text, re.I):
                first = having.strip().split()[0].strip("(")
                if first.lower() in {a.lower() for a in aliases}:
                    offenders.append((lineno, having.strip()[:60]))
        assert not offenders, (
            f"HAVING referring to a select alias, which PostgreSQL rejects: "
            f"{offenders}"
        )

    def test_no_arbitrary_pick_decides_a_name(self):
        """`any_value` returns *an* value from the group, and the two engines
        are free to pick different ones — so the same product would carry
        different names on the two sides and the comparison would be
        measuring the planners."""
        for lineno, text in self._statements():
            assert "any_value" not in text.lower(), (
                f"line {lineno} lets the engine choose which name to show")

    def test_no_method_opens_the_store_itself(self):
        src = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if not (isinstance(node, ast.AsyncFunctionDef)
                    and node.name.startswith("get_")):
                continue
            body = ast.get_source_segment(src, node) or ""
            assert "self.connection()" not in body, f"{node.name} still opens DuckDB"
            assert "conn.execute" not in body, f"{node.name} bypasses the router"

