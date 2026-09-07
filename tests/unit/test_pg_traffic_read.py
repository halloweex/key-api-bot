"""Which engine answers `/traffic`, and the four defects the port exposed.

The numbers are `tests/integration/test_traffic_two_engines.py`'s job. What is
proved here is the wiring, plus the mistakes worth a permanent guard — three of
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
import asyncio
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from core import pg_traffic_read
from core.duckdb_store import DuckDBStore
from core.sql_dialect import DUCKDB, POSTGRES, render_tables

REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "traffic.py"
W = (date.today() - timedelta(days=30), date.today())
TIMEOUT_S = 20

CALLS = (
    ("get_traffic_analytics", {}),
    ("get_traffic_trend", {}),
    ("get_traffic_transactions", {}),
    ("get_traffic_utm_campaigns", {}),
    ("get_traffic_roas", {}),
)


class TestTheFlag:
    def test_it_defaults_to_duckdb(self, monkeypatch):
        monkeypatch.delenv("KS_READ_TRAFFIC", raising=False)
        assert pg_traffic_read.enabled() is False

    def test_postgres_turns_it_on(self, monkeypatch):
        monkeypatch.setenv("KS_READ_TRAFFIC", "postgres")
        assert pg_traffic_read.enabled() is True

    def test_a_typo_raises(self, monkeypatch):
        monkeypatch.setenv("KS_READ_TRAFFIC", "postgress")
        with pytest.raises(ValueError, match="KS_READ_TRAFFIC"):
            pg_traffic_read.enabled()


class TestEveryMethodActuallyRuns:
    """The guard the f-string brace earned.

    A hole written with single braces parses, reads correctly to a human, and
    fails only on the call — so all five are called for real, against an empty
    DuckDB. The timeout is the second assertion: the store lock is not
    reentrant and a nested acquisition hangs rather than raising.
    """

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name,kwargs", CALLS, ids=[n for n, _ in CALLS])
    async def test_it_returns_rather_than_raising_or_hanging(
        self, tmp_path, monkeypatch, name, kwargs,
    ):
        monkeypatch.delenv("KS_READ_TRAFFIC", raising=False)
        store = DuckDBStore(db_path=tmp_path / f"t-{name}.duckdb")
        await store.connect()
        try:
            await asyncio.wait_for(
                getattr(store, name)(*W, **kwargs), timeout=TIMEOUT_S,
            )
        finally:
            await store.close()


class TestTheRoutedBodies:
    def _statements(self):
        """Every SQL literal inside a `get_traffic_*` method, as literal text.

        Not the routed call's argument: four of the five methods build their
        statement into a local first and pass the name, so reading arguments
        finds three of the nine. Reading the method bodies finds all of them —
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

    def test_the_scan_finds_all_nine(self):
        """Five methods, nine statements: transactions and campaigns each run
        a count beside their page of rows, and ROAS runs three — total
        revenue, paid revenue per platform, and the ad spend."""
        assert len(list(self._statements())) == 9

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


class TestItDoesNotTakeTheDuckDbLock:
    @pytest.mark.asyncio
    async def test_a_postgres_read_answers_while_the_store_lock_raises(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setenv("KS_READ_TRAFFIC", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / "tr.duckdb")
        await store.connect()
        try:
            def boom(*_a, **_k):
                raise AssertionError("the traffic read reached for DuckDB")

            with patch.object(type(store), "connection", boom), \
                 patch("core.pg_traffic_read.fetch",
                       new=AsyncMock(return_value=[])) as fetch:
                await store.get_traffic_analytics(*W)
            sql = fetch.await_args.args[0]
            assert "silver.orders" in sql and "silver.order_utm" in sql
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_duckdb_path_still_uses_the_store(self, tmp_path, monkeypatch):
        """The mirror, so the test above cannot pass by the routing being
        broken in both directions."""
        monkeypatch.delenv("KS_READ_TRAFFIC", raising=False)
        store = DuckDBStore(db_path=tmp_path / "tr2.duckdb")
        await store.connect()
        try:
            with patch("core.pg_traffic_read.fetch",
                       new=AsyncMock(return_value=[])) as fetch:
                await store.get_traffic_analytics(*W)
            fetch.assert_not_awaited()
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_postgres_fault_falls_back_to_duckdb(self, tmp_path, monkeypatch):
        monkeypatch.setenv("KS_READ_TRAFFIC", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / "tr3.duckdb")
        await store.connect()
        try:
            with patch("core.pg_traffic_read.fetch",
                       new=AsyncMock(side_effect=RuntimeError("pg is down"))):
                out = await store.get_traffic_analytics(*W)
            assert out["totals"]["orders"] == 0
        finally:
            await store.close()
