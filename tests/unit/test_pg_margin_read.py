"""The `/margin` statements, and the brace that would have broken them.

The numbers are `tests/integration/test_margin_two_engines.py`'s job, against
a real Postgres, which calls every method for real; that the router asks
Postgres and nothing else since step 14's S14-05 is
`tests/unit/test_read_routers_postgres_only.py`'s. What is proved here is the
statements, plus one mistake worth a permanent guard: the bodies
are f-strings, so a table hole written `{order_lines}` is an f-string
*expression*, not a hole. It parses, it passes a structural review, and it
raises `NameError` the first time the method is called. Only `{{order_lines}}`
survives into the rendered SQL — and only actually calling the method finds the
difference.
"""
from __future__ import annotations

import ast
from pathlib import Path

from core.sql_dialect import POSTGRES, render_tables

REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "margin.py"


class TestTheRoutedBodies:
    def _statements(self):
        src = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if not (isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Attribute)
                    and node.func.attr == "_margin_run"):
                continue
            for part in ast.walk(node):
                if isinstance(part, ast.JoinedStr):
                    text = "".join(
                        v.value for v in part.values
                        if isinstance(v, ast.Constant) and isinstance(v.value, str)
                    )
                    if "SELECT" in text.upper():
                        yield node.lineno, text

    def test_the_scan_finds_all_six(self):
        assert len(list(self._statements())) == 6

    def test_every_hole_survived_the_f_string(self):
        """`{order_lines}` inside an f-string is an expression that Python
        evaluates at runtime; only `{{order_lines}}` reaches the renderer as a
        hole. The scan above reads the *literal segments*, so a single brace
        simply would not appear here."""
        for lineno, text in self._statements():
            assert "{order_lines}" in text, (
                f"line {lineno} has no table hole — either it does not read the "
                f"level, or the brace was written singly and Python ate it"
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
                    and node.name.startswith("get_margin")):
                continue
            body = ast.get_source_segment(src, node) or ""
            assert "self.connection()" not in body, f"{node.name} still opens DuckDB"
            assert "conn.execute" not in body, f"{node.name} bypasses the router"


class TestNoOrderIsLeftToThePlanner:
    """Every ordered result ends on a key that cannot tie.

    The gate found this the hard way: two categories on ₴1 450 each came back
    in opposite orders from the two engines. Two of these queries also carry a
    `LIMIT`, where a tie inside the cut means different *rows* rather than the
    same rows reordered — a brand quietly missing from the page.

    `1` (and `2` in the cross-tab) is the grouping key, which is unique per
    output row by construction, so it is always a sufficient tiebreaker here.
    """

    UNIQUE_TAILS = ("1", "2")

    def test_every_ordered_query_ends_on_the_grouping_key(self):
        offenders = []
        for lineno, text in TestTheRoutedBodies()._statements():
            body = "\n".join(l.split("--")[0] for l in text.splitlines())
            upper = body.upper()
            if "ORDER BY" not in upper:
                continue
            tail = body[upper.rindex("ORDER BY") + 8:]
            tail = tail.split("LIMIT")[0].split("\n\n")[0]
            last = [c for c in tail.split(",") if c.strip()][-1].strip().split()[0]
            if last not in self.UNIQUE_TAILS:
                offenders.append((lineno, tail.strip()))
        assert not offenders, (
            f"ORDER BY can tie, so the two engines may disagree about which "
            f"rows a LIMIT keeps: {offenders}"
        )

    def test_the_scan_would_have_caught_the_original(self):
        """A guard on the guard: the check must actually reject the shape the
        gate found, or it is decoration."""
        sample = "SELECT x, SUM(y) AS total_revenue FROM t GROUP BY 1 ORDER BY total_revenue DESC"
        upper = sample.upper()
        tail = sample[upper.rindex("ORDER BY") + 8:]
        last = [c for c in tail.split(",") if c.strip()][-1].strip().split()[0]
        assert last not in self.UNIQUE_TAILS
