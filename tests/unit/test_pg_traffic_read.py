"""The `/traffic` statements, and the four defects the port exposed.

The numbers are `tests/integration/test_traffic_two_engines.py`'s job, against
a real Postgres; that the router asks Postgres and nothing else since step
14's S14-05 is `tests/unit/test_read_routers_postgres_only.py`'s. What is
proved here is the statements, plus the mistakes worth a permanent guard — three of
them found by reading the queries while porting them, and each one a real
defect on DuckDB too, not merely an incompatibility:

* `any_value(u.utm_source)` — nondeterministic by definition, on a column that
  is not grouped on;
* `SELECT COUNT(*) FROM ( ... )` with the derived table unnamed, which
  PostgreSQL rejects outright;
* `ORDER BY <sort column>` with no tiebreaker under `LIMIT`/`OFFSET`, so two
  campaigns tied on revenue can land on both pages or on neither.

The fourth is the one that would have shipped silently: the blended ROAS sums
`gold_daily_revenue`, whose Postgres shape carries the source as a dimension
*and* a roll-up row, so a bare sum doubles there.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

from core.sql_dialect import POSTGRES, render_tables

REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "traffic.py"


class TestTheRoutedBodies:
    def _statements(self):
        """Every SQL literal inside a `get_traffic_*` method, as literal text.

        Not the routed call's argument: four of the five methods build their
        statement into a local first and pass the name, so reading arguments
        finds four of the ten. Reading the method bodies finds all of them —
        and it is the stricter scan anyway, because a query that stopped being
        routed would still be caught here.

        Constants nested inside an f-string are skipped explicitly. Python
        folds adjacent literals into one node, so a plain string concatenated
        with an f-string is a single `JoinedStr` whose children `walk` would
        otherwise visit a second time and count twice.
        """
        src = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(src)
        for fn in ast.walk(tree):
            if not (isinstance(fn, ast.AsyncFunctionDef)
                    and fn.name.startswith("get_traffic")):
                continue
            nested = {
                id(v) for node in ast.walk(fn)
                if isinstance(node, ast.JoinedStr) for v in node.values
            }
            for node in ast.walk(fn):
                if isinstance(node, ast.JoinedStr):
                    text = "".join(
                        v.value for v in node.values
                        if isinstance(v, ast.Constant) and isinstance(v.value, str)
                    )
                elif (isinstance(node, ast.Constant)
                      and isinstance(node.value, str)
                      and id(node) not in nested):
                    text = node.value
                else:
                    continue
                if "SELECT" in text.upper():
                    yield node.lineno, text

    def test_the_scan_finds_all_twelve(self):
        """Six methods, twelve statements: analytics and the organic breakdown
        each run their new buyers beside their orders (the breakdown runs its
        orders statement for two periods, but it is one text), transactions
        and campaigns each run a count beside their page of rows, and ROAS
        runs three — total revenue, paid revenue per platform, and the ad
        spend."""
        assert len(list(self._statements())) == 12

    def test_every_hole_survived_the_f_string(self):
        """`{silver_orders}` inside an f-string is an expression Python
        evaluates at runtime; only `{{silver_orders}}` reaches the renderer as
        a hole. The scan reads the *literal segments*, so a single brace simply
        would not appear here."""
        for lineno, text in self._statements():
            assert "{" in text, (
                f"line {lineno} names no table hole — either it does not read a "
                f"level, or a brace was written singly and Python ate it"
            )

    def test_the_rendering_leaves_no_hole(self):
        for lineno, text in self._statements():
            rendered = render_tables(text, POSTGRES)
            assert "{" not in rendered and "}" not in rendered, (
                f"line {lineno} unrendered for {POSTGRES.name}"
            )

    def test_no_method_opens_the_store_itself(self):
        src = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if not (isinstance(node, ast.AsyncFunctionDef)
                    and node.name.startswith("get_traffic")):
                continue
            body = ast.get_source_segment(src, node) or ""
            assert "self.connection()" not in body, f"{node.name} still opens DuckDB"
            assert "conn.execute" not in body, f"{node.name} bypasses the router"


class TestTheDefectsThePortExposed:
    def _read_bodies(self):
        return [t for _, t in TestTheRoutedBodies()._statements()]

    def test_no_query_uses_any_value(self):
        """`any_value` is DuckDB-only *and* nondeterministic: it returns
        whichever row the engine reached first, which is not the same row in
        two engines and need not be the same row twice in one."""
        for text in self._read_bodies():
            assert "any_value" not in text.lower(), text[:120]

    def test_every_derived_table_is_named(self):
        """PostgreSQL requires an alias on a subquery in FROM; DuckDB does
        not. One body has to satisfy the stricter of the two."""
        for text in self._read_bodies():
            for chunk in text.split(")")[:-1]:
                if chunk.rstrip().endswith("FROM ("):
                    pytest.fail(f"unnamed derived table: {text[:160]}")

    def test_the_revenue_gold_is_never_summed_bare(self):
        """DuckDB's revenue Gold has one row per (date, sales_type); Postgres
        adds `source_id` with the roll-up as its own row. A sum without the
        predicate reads correctly in one engine and doubles in the other."""
        for text in self._read_bodies():
            if "{gold_daily_revenue}" in text:
                assert "{gold_revenue_rollup}" in text, (
                    "summing the revenue Gold without the roll-up predicate "
                    "double-counts every order in Postgres"
                )

    def test_the_traffic_gold_is_no_longer_read(self):
        """The whole reason this store needs no traffic Gold: both readers
        folded it, and a fold of cells keyed 1:1 on the order is the same
        arithmetic as the aggregate over the orders."""
        src = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if not (isinstance(node, ast.AsyncFunctionDef)
                    and node.name.startswith("get_traffic")):
                continue
            body = ast.get_source_segment(src, node) or ""
            assert "gold_daily_traffic" not in body, (
                f"{node.name} reads a Gold that exists in only one engine"
            )


class TestNoOrderIsLeftToThePlanner:
    """Every ordered result ends on a key that cannot tie.

    The campaign table is paginated, so a tie inside the cut means different
    *rows* rather than the same rows reordered — a campaign quietly missing
    from the page. The grouping keys are unique per output row by
    construction, so they are always a sufficient tiebreaker.
    """

    UNIQUE_TAILS = ("platform", "traffic_type", "s.id", "campaign")

    def test_every_ordered_query_ends_on_a_key(self):
        offenders = []
        for lineno, text in TestTheRoutedBodies()._statements():
            head, sep, tail = text.upper().rpartition("ORDER BY")
            if not sep:
                continue
            clause = tail.split("LIMIT")[0].strip()
            last = clause.split(",")[-1].strip().rstrip(")").split()[0]
            if last.lower() not in [t.lower() for t in self.UNIQUE_TAILS]:
                offenders.append((lineno, clause))
        assert not offenders, f"ORDER BY can tie: {offenders}"

