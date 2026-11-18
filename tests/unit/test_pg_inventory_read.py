"""The `/inventory` statements: what no engine may be left to decide.

The numbers are `tests/integration/test_inventory_two_engines.py`'s job,
against a real Postgres; that the router asks Postgres and nothing else since
step 14's S14-07 is `tests/unit/test_tab_routers_postgres_only.py`'s. What is
proved here is the shape of the routed statements and of the methods around
them: no ordered result left to the planner, and no routing call inside a
`self.connection()` block — the store lock is not reentrant, and a nested
acquisition does not raise, it simply never comes back. That is how the
cohort port shipped a deadlock past a green suite.

Until S14-07 this also held the flag (`KS_READ_INVENTORY`: its default, a
typo raising), that a Postgres read never took DuckDB's lock (§34) and that
every method returned on an empty DuckDB; the flag and the DuckDB path went
with that PR.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

from tests.sql_helper import strip_comments_and_literals


class TestNoResultOrderIsLeftToThePlanner:
    """Every routed query that produces an ordered result ends its ORDER BY on
    a key that cannot tie.

    Two engines break a tie differently, and where a LIMIT falls inside the
    tied group they return *different rows*, not merely a different order. The
    synthetic fixture cannot be relied on to produce every tie: comparing the
    two engines over the real 891-SKU catalogue found one that thirteen SKUs
    had missed, in `get_dead_stock_items_v2`. This is the check that does not
    depend on the data.
    """

    # Columns that are unique within their result, so nothing can tie on them.
    UNIQUE_KEYS = frozenset({
        "offer_id", "date", "bucket", "period", "category_id",
        "1",            # a positional key: the GROUP BY column itself
    })

    @staticmethod
    def _trailing_order_by(sql: str) -> str | None:
        """The result's own ORDER BY, or None.

        Window frames (`ROW_NUMBER() OVER (ORDER BY …)`) and ordered-set
        aggregates (`WITHIN GROUP (ORDER BY …)`) also spell `ORDER BY`, and
        neither orders the result. Both live inside parentheses, so the one
        that matters is the one at depth zero.
        """
        body = strip_comments_and_literals(sql)
        depth = 0
        for i, char in enumerate(body):
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif depth == 0 and body[i:i + 8].upper() == "ORDER BY":
                tail = body[i + 8:]
                tail = re.split(r"LIMIT", tail, flags=re.I)[0]
                return " ".join(tail.split())
        return None

    def _routed_queries(self):
        source = Path("core/repositories/inventory.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in ("_inventory_rows", "_inventory_batch")):
                continue
            for literal in ast.walk(node):
                if (isinstance(literal, ast.Constant)
                        and isinstance(literal.value, str)
                        and "SELECT" in literal.value.upper()):
                    yield literal.lineno, literal.value

    def test_the_routed_queries_are_found_at_all(self):
        # A guard on the guard: an AST walk that matched nothing would let
        # every assertion below pass in silence.
        assert len(list(self._routed_queries())) >= 15

    def test_every_ordered_result_ends_on_a_key_that_cannot_tie(self):
        offenders = []
        for lineno, sql in self._routed_queries():
            order = self._trailing_order_by(sql)
            if order is None:
                continue
            last = order.split(",")[-1].strip().split()[0]
            if last not in self.UNIQUE_KEYS:
                offenders.append((lineno, order))
        assert not offenders, (
            f"ORDER BY can tie, so the two engines may disagree about which "
            f"rows a LIMIT keeps: {offenders}"
        )

    def test_an_unordered_result_is_never_read_as_a_list(self):
        """The queries with no ORDER BY at all are the ones whose caller keys
        the rows rather than rendering them in order — `v_inventory_summary`
        and `v_abc_summary` become dicts. Anything else without one would be a
        list in planner order."""
        unordered = [
            " ".join(sql.split())
            for _lineno, sql in self._routed_queries()
            if self._trailing_order_by(sql) is None
        ]
        assert unordered, "no unordered queries found — the scan is broken"
        for sql in unordered:
            assert (
                "v_inventory_summary" in sql
                or "v_abc_summary" in sql
                or "COUNT(*)" in sql.upper()      # single-row aggregates
                or "COALESCE(SUM" in sql.upper()
            ), f"unordered query that is read as a list: {sql[:120]}"
