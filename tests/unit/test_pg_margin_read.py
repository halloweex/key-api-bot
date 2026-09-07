"""Which engine answers `/margin`, and the brace that would have broken it.

The numbers are `tests/integration/test_margin_two_engines.py`'s job. What is
proved here is the wiring, plus one mistake worth a permanent guard: the bodies
are f-strings, so a table hole written `{order_lines}` is an f-string
*expression*, not a hole. It parses, it passes a structural review, and it
raises `NameError` the first time the method is called. Only `{{order_lines}}`
survives into the rendered SQL — and only actually calling the method finds the
difference.
"""
from __future__ import annotations

import ast
import asyncio
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_margin_read
from core.duckdb_store import DuckDBStore
from core.sql_dialect import DUCKDB, POSTGRES, render_tables

REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "margin.py"
W = (date.today() - timedelta(days=30), date.today())
TIMEOUT_S = 20

CALLS = (
    ("get_margin_overview", {}),
    ("get_margin_by_brand", {}),
    ("get_margin_by_category", {}),
    ("get_margin_trend", {}),
    ("get_margin_brand_category", {"min_revenue": 0}),
    ("get_margin_alerts", {"min_revenue": 0}),
)


class TestTheFlag:
    def test_it_defaults_to_duckdb(self, monkeypatch):
        monkeypatch.delenv("KS_READ_MARGIN", raising=False)
        assert pg_margin_read.enabled() is False

    def test_postgres_turns_it_on(self, monkeypatch):
        monkeypatch.setenv("KS_READ_MARGIN", "postgres")
        assert pg_margin_read.enabled() is True

    def test_a_typo_raises(self, monkeypatch):
        monkeypatch.setenv("KS_READ_MARGIN", "postgress")
        with pytest.raises(ValueError, match="KS_READ_MARGIN"):
            pg_margin_read.enabled()


class TestEveryMethodActuallyRuns:
    """The guard the f-string brace earned.

    A hole written with single braces parses, reads correctly to a human, and
    fails only on the call — so these six are called for real, against an empty
    DuckDB. The timeout is the second assertion: the store lock is not
    reentrant and a nested acquisition hangs rather than raising.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name,kwargs", CALLS, ids=[n for n, _ in CALLS])
    async def test_it_returns_rather_than_raising_or_hanging(
        self, tmp_path, monkeypatch, name, kwargs,
    ):
        monkeypatch.delenv("KS_READ_MARGIN", raising=False)
        store = DuckDBStore(db_path=tmp_path / f"m-{name}.duckdb")
        await store.connect()
        try:
            await asyncio.wait_for(
                getattr(store, name)(*W, **kwargs), timeout=TIMEOUT_S,
            )
        finally:
            await store.close()


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

    def test_both_renderings_leave_no_hole(self):
        for lineno, text in self._statements():
            for dialect in (DUCKDB, POSTGRES):
                rendered = render_tables(text, dialect)
                assert "{" not in rendered and "}" not in rendered, (
                    f"line {lineno} unrendered for {dialect.name}"
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


class TestItDoesNotTakeTheDuckDbLock:
    @pytest.mark.asyncio
    async def test_a_postgres_read_answers_while_the_store_lock_raises(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setenv("KS_READ_MARGIN", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / "mg.duckdb")
        await store.connect()
        try:
            def boom(*_a, **_k):
                raise AssertionError("the margin read reached for DuckDB")

            with patch.object(type(store), "connection", boom), \
                 patch("core.pg_margin_read.fetch",
                       new=AsyncMock(return_value=[])) as fetch:
                await store.get_margin_by_brand(*W)
            sql = fetch.await_args.args[0]
            assert "silver.order_lines" in sql and "bronze.offer_stocks" in sql
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_duckdb_path_still_uses_the_store(self, tmp_path, monkeypatch):
        """The mirror, so the test above cannot pass by the routing being
        broken in both directions."""
        monkeypatch.delenv("KS_READ_MARGIN", raising=False)
        store = DuckDBStore(db_path=tmp_path / "mg2.duckdb")
        await store.connect()
        try:
            with patch("core.pg_margin_read.fetch",
                       new=AsyncMock(return_value=[])) as fetch:
                await store.get_margin_by_brand(*W)
            fetch.assert_not_awaited()
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_postgres_fault_falls_back_to_duckdb(self, tmp_path, monkeypatch):
        monkeypatch.setenv("KS_READ_MARGIN", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / "mg3.duckdb")
        await store.connect()
        try:
            with patch("core.pg_margin_read.fetch",
                       new=AsyncMock(side_effect=RuntimeError("pg is down"))):
                out = await store.get_margin_overview(*W)
            assert out["total_revenue"] == 0
        finally:
            await store.close()


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
