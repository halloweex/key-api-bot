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


class TestTheHelperIsNeverCalledUnderTheLock:
    """The structural half of the test above: the timeout catches a deadlock
    on a path a test walks, this catches the shape anywhere."""

    def test_no_routing_call_sits_inside_a_connection_block(self):
        source = Path("core/repositories/inventory.py").read_text(encoding="utf-8")
        tree = ast.parse(source)

        def opens_the_store(node: ast.AST) -> bool:
            for item in getattr(node, "items", []):
                call = item.context_expr
                if (isinstance(call, ast.Call)
                        and isinstance(call.func, ast.Attribute)
                        and call.func.attr == "connection"):
                    return True
            return False

        offenders = []
        for node in ast.walk(tree):
            if not isinstance(node, (ast.With, ast.AsyncWith)) or not opens_the_store(node):
                continue
            for inner in ast.walk(node):
                if (isinstance(inner, ast.Call)
                        and isinstance(inner.func, ast.Attribute)
                        and inner.func.attr in ("_inventory_rows", "_inventory_batch")):
                    offenders.append(inner.lineno)
        assert not offenders, (
            f"routing helper called inside `self.connection()` at line(s) "
            f"{offenders} — the store lock is not reentrant and the deadlock "
            f"will not raise"
        )

    def test_every_view_reader_goes_through_the_helper(self):
        """A method left on `conn.execute` against a view would keep working
        on DuckDB and silently ignore the flag — the half-switched state
        `refuse_while_unported` exists to prevent on the `/sms` side."""
        source = Path("core/repositories/inventory.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        stragglers = []
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr in ("execute", "executemany")):
                continue
            for arg in node.args[:1]:
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    if "v_" in arg.value and "FROM" in arg.value.upper():
                        stragglers.append((node.lineno, arg.value.strip()[:60]))
        assert not stragglers, f"view read outside the routing helper: {stragglers}"

    def test_no_reader_names_an_unrouted_table(self):
        """The boundary this test used to guard is gone, and this is what
        replaced it.

        Until 2026-09-06 `/api/stocks/summary` was the one read route left on
        DuckDB, and the recorded reason was that it joined `offers` and
        `sync_metadata`, neither of which is in Postgres. Both halves turned
        out to be avoidable rather than blocking — the join to `offers` existed
        only to fetch `p.name`, which `sku_inventory_status` already carries
        from the identical joins, and the `sync_metadata` row was a key nothing
        in the repository has ever written, so `lastSync` had been NULL since
        the feature shipped. The port needed no replication at all.

        So the check is now the general one: no read method may name a table
        that exists only in DuckDB. A writer may — it writes there.
        """
        source = Path("core/repositories/inventory.py").read_text(encoding="utf-8")
        tree = ast.parse(source)

        # Tables DuckDB has and Postgres does not. `offers` and `sync_metadata`
        # are landing/control state nobody replicated; adding one here is how
        # the next port learns it has a dependency to resolve first.
        DUCKDB_ONLY = ("FROM offers", "JOIN offers", "FROM sync_metadata")

        offenders = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.AsyncFunctionDef):
                continue
            if not node.name.startswith("get_"):
                continue          # writers and refreshers keep their own store
            body = ast.get_source_segment(source, node) or ""
            for name in DUCKDB_ONLY:
                if name in body:
                    offenders.append((node.name, name))
        assert not offenders, (
            f"read method naming a table Postgres does not have: {offenders} — "
            f"the flag cannot switch it, and it will answer from DuckDB while "
            f"the rest of the tab answers from Postgres"
        )

    def test_the_summary_reads_history_through_the_helper(self):
        """`inventory_history` has two readers and only one may hold a
        connection: the writer that records the snapshot."""
        source = Path("core/repositories/inventory.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        readers = set()
        for node in ast.walk(tree):
            if not isinstance(node, ast.AsyncFunctionDef):
                continue
            body = ast.get_source_segment(source, node) or ""
            if "{inventory_history}" in body or "FROM inventory_history" in body:
                readers.add(node.name)
        assert "get_stock_summary" in readers
        assert "get_average_inventory" in readers
        # The writer is the only one allowed to name it unrendered.
        unrendered = {
            n.name for n in ast.walk(tree)
            if isinstance(n, ast.AsyncFunctionDef)
            and "FROM inventory_history" in (ast.get_source_segment(source, n) or "")
        }
        assert unrendered == {"record_inventory_snapshot"}, unrendered
