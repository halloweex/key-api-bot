"""Which engine answers `/expenses`, and the fan-out the port found.

The numbers are `tests/integration/test_expenses_two_engines.py`'s job. What
is proved here is the wiring, the shared parse, and one defect that was live on
production before this port touched anything:

`get_profit_analysis` read `FROM orders o LEFT JOIN expenses e` and summed
`o.grand_total` over the joined rows, so an order carrying two expenses had its
revenue counted twice. 157 orders have more than one. Measured on the
production catalogue: ₴16,433,644.56 reported over 90 days against a true
₴16,119,279.06 — ₴314,365.50 too much, and ₴62,000 on 2026-07-28 alone.
"""
from __future__ import annotations

import ast
import asyncio
import inspect
import textwrap
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from core import pg_expenses_read
from core.duckdb_store import DuckDBStore
from core.sql_dialect import DUCKDB, POSTGRES, render_tables

REPO = Path(__file__).resolve().parents[2]
REPOSITORY = REPO / "core" / "repositories" / "expenses.py"
W = (date.today() - timedelta(days=30), date.today())
TIMEOUT_S = 20

READS = (
    ("get_expense_types", (), {}),
    ("get_expense_summary", W, {}),
    ("get_profit_analysis", W, {}),
    ("list_expenses", (), {}),
    ("get_ad_spend_by_platform", W, {}),
    ("get_expenses_summary", (), {}),
)


class TestTheFlag:
    def test_it_defaults_to_duckdb(self, monkeypatch):
        monkeypatch.delenv("KS_READ_EXPENSES", raising=False)
        assert pg_expenses_read.enabled() is False

    def test_postgres_turns_it_on(self, monkeypatch):
        monkeypatch.setenv("KS_READ_EXPENSES", "postgres")
        assert pg_expenses_read.enabled() is True

    def test_a_typo_raises(self, monkeypatch):
        monkeypatch.setenv("KS_READ_EXPENSES", "postgress")
        with pytest.raises(ValueError, match="KS_READ_EXPENSES"):
            pg_expenses_read.enabled()


class TestEveryMethodActuallyRuns:
    """A hole written with single braces parses, reads correctly to a human,
    and fails only on the call. The timeout is the second assertion: the store
    lock is not reentrant and a nested acquisition hangs rather than raising."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name,args,kwargs", READS, ids=[r[0] for r in READS])
    async def test_it_returns_rather_than_raising_or_hanging(
        self, tmp_path, monkeypatch, name, args, kwargs,
    ):
        monkeypatch.delenv("KS_READ_EXPENSES", raising=False)
        store = DuckDBStore(db_path=tmp_path / f"e-{name}.duckdb")
        await store.connect()
        try:
            await asyncio.wait_for(
                getattr(store, name)(*args, **kwargs), timeout=TIMEOUT_S,
            )
        finally:
            await store.close()


class TestTheRoutedBodies:
    def _statements(self):
        src = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(src)
        for fn in ast.walk(tree):
            if not (isinstance(fn, ast.AsyncFunctionDef)
                    and fn.name in {r[0] for r in READS}):
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
                    yield fn.name, node.lineno, text

    def test_every_hole_survived_the_f_string(self):
        for name, lineno, text in self._statements():
            assert "{" in text, f"{name}:{lineno} names no table hole"

    def test_both_renderings_leave_no_hole(self):
        for name, lineno, text in self._statements():
            for dialect in (DUCKDB, POSTGRES):
                rendered = render_tables(text, dialect)
                assert "{" not in rendered and "}" not in rendered, (
                    f"{name}:{lineno} unrendered for {dialect.name}"
                )

    def test_no_read_opens_the_store_or_bypasses_the_router(self):
        src = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(src)
        for fn in ast.walk(tree):
            if not (isinstance(fn, ast.AsyncFunctionDef)
                    and fn.name in {r[0] for r in READS}):
                continue
            for node in ast.walk(fn):
                if isinstance(node, ast.AsyncWith):
                    head = ast.unparse(node.items[0].context_expr)
                    assert "connection()" not in head, f"{fn.name} opens DuckDB"
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    assert node.func.attr != "execute", f"{fn.name} bypasses the router"

    def test_the_router_keeps_the_offloading_helpers(self):
        """`/traffic` lost these by writing the obvious thing; the DuckDB
        branch is what answers when Postgres is down, and it must not block
        the event loop without a bound."""
        from core.repositories.expenses import ExpensesMixin

        src = inspect.getsource(ExpensesMixin._expenses_run)
        assert "self._fetch_one(" in src and "self._fetch_all(" in src
        for node in ast.walk(ast.parse(textwrap.dedent(src))):
            if isinstance(node, ast.AsyncWith):
                head = ast.unparse(node.items[0].context_expr)
                assert "connection()" not in head, "nested store-lock acquisition"


class TestTheFanOutIsFixed:
    """The defect this port surfaced, pinned so it cannot return."""

    def _profit_sql(self):
        return [t for n, _, t in TestTheRoutedBodies()._statements()
                if n == "get_profit_analysis"]

    def test_the_revenue_query_folds_expenses_before_joining(self):
        sql = self._profit_sql()
        assert sql, "get_profit_analysis issues no statement"
        body = " ".join(sql)
        assert "GROUP BY order_id" in body, (
            "expenses must be aggregated per order before the join, or an "
            "order with two of them has its revenue counted twice"
        )

    def test_it_no_longer_joins_expenses_directly_to_orders(self):
        for text in self._profit_sql():
            compact = " ".join(text.split()).upper()
            assert "LEFT JOIN {EXPENSES} E ON" not in compact


class TestTheSharedParse:
    """`expense_types` is the first landing table whose parse is a real
    transformation, so it had to move before the table gained a second
    writer."""

    def test_the_localisation_key_is_resolved_from_the_alias(self):
        from core.landing_rows import expense_type_row

        row = expense_type_row({
            "id": 1, "name": "dictionaries.expense_types.delivery",
            "alias": "delivery", "is_active": True,
        })
        assert row.name == "Delivery"

    def test_without_an_alias_it_falls_back_to_the_key_itself(self):
        from core.landing_rows import expense_type_row

        row = expense_type_row({
            "id": 2, "name": "dictionaries.expense_types.courier_delivery",
            "alias": None,
        })
        assert row.name == "Courier Delivery"

    def test_an_ordinary_name_is_left_alone(self):
        from core.landing_rows import expense_type_row

        assert expense_type_row({"id": 9, "name": "Мито"}).name == "Мито"

    @pytest.mark.parametrize("raw,expected_kind", [
        ("2026-09-07T14:31:22.000000Z", "datetime"),
        ("2026-09-05 13:04:29", "datetime"),
        ("2026-09-05", "datetime"),
        (None, "none"),
        ("", "none"),
        ("not a date", "none"),
    ])
    def test_timestamps_are_parsed_not_passed_through(self, raw, expected_kind):
        """KeyCRM sends ISO strings; asyncpg refuses them for TIMESTAMPTZ and
        DuckDB accepts them, so passing the payload value straight through
        wrote one store and failed the other — 66 rows on the first batch in
        production. A parse is not finished until both stores accept it."""
        from datetime import datetime

        from core.landing_rows import expense_row

        row = expense_row(1, {"id": 9, "payment_date": raw, "created_at": raw})
        for value in (row.payment_date, row.created_at):
            if expected_kind == "datetime":
                assert isinstance(value, datetime), f"{raw!r} -> {value!r}"
            else:
                assert value is None

    def test_a_null_expenses_list_is_not_a_crash(self):
        """`order.get("expenses", [])` raised on `"expenses": null`, which
        KeyCRM is free to send. The flattening lives in one place now and
        treats it as no expenses."""
        from core.landing_rows import expense_rows

        assert expense_rows([{"id": 1, "expenses": None}]) == []

    def test_the_store_writes_the_shared_rows(self):
        """Not a second reading of the payload — that is what charter rule 1
        forbids, and it is what would let the two stores disagree about what
        an expense is called."""
        src = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(src)
        for fn in ast.walk(tree):
            if not (isinstance(fn, ast.AsyncFunctionDef)
                    and fn.name in {"upsert_expense_types", "upsert_expenses_batch"}):
                continue
            body = ast.get_source_segment(src, fn) or ""
            assert "landing_rows" in body, f"{fn.name} parses the payload itself"
            assert "dictionaries.expense_types." not in body, (
                f"{fn.name} still carries its own copy of the name cleanup"
            )


class TestAWriteReachesPostgresAtOnce:
    """The lag revision 0018 accepted stops being acceptable once the page
    reads the other store."""

    WRITES = ("add_expense", "update_expense", "delete_expense")

    def test_every_write_replicates(self):
        src = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(src)
        for fn in ast.walk(tree):
            if not (isinstance(fn, ast.AsyncFunctionDef) and fn.name in self.WRITES):
                continue
            calls = {
                getattr(n.func, "id", None)
                for n in ast.walk(fn) if isinstance(n, ast.Call)
            }
            assert "replicate_after_manual_expense" in calls, fn.name

    def test_none_of_them_replicates_under_the_store_lock(self):
        """It awaits the network, and that lock is global."""
        src = REPOSITORY.read_text(encoding="utf-8")
        tree = ast.parse(src)
        for fn in ast.walk(tree):
            if not (isinstance(fn, ast.AsyncFunctionDef) and fn.name in self.WRITES):
                continue
            for node in ast.walk(fn):
                if not (isinstance(node, ast.AsyncWith)
                        and "connection()" in ast.unparse(node.items[0].context_expr)):
                    continue
                for inner in ast.walk(node):
                    if (isinstance(inner, ast.Call)
                            and getattr(inner.func, "id", None) == "replicate_after_manual_expense"):
                        pytest.fail(f"{fn.name} replicates while holding the store")

    @pytest.mark.asyncio
    async def test_it_never_raises(self):
        from core.pg_operational import replicate_after_manual_expense

        with patch("core.mirror_reconciliation.configured", return_value=True), \
             patch("core.pg_operational.replicate_operational",
                   new=AsyncMock(side_effect=RuntimeError("pg is down"))):
            out = await replicate_after_manual_expense(AsyncMock())
        assert "error" in out


class TestHistoryMustBeAcrossBeforePostgresAnswers:
    """The mechanism that replaces "run the backfill first".

    On 2026-09-07 the flag went on while Postgres held 65 of 15,020 expenses,
    and the tab answered from it without a murmur — an empty result is not an
    exception, so nothing fell back and nothing was logged. The instruction had
    been written down, said out loud, and put in the finding's own text. None
    of that is a mechanism; `backfilled_at` is.
    """

    def setup_method(self):
        from core import pg_expenses_read as r

        r._backfilled = False
        r._checked_at = 0.0

    @pytest.mark.asyncio
    async def test_a_body_reading_the_landing_waits_for_history(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setenv("KS_READ_EXPENSES", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / "h1.duckdb")
        await store.connect()
        try:
            with patch("core.pg_expenses_read.backfilled",
                       new=AsyncMock(return_value=False)), \
                 patch("core.pg_expenses_read.fetch",
                       new=AsyncMock(return_value=[])) as fetch:
                await store.get_expense_summary(*W)
            fetch.assert_not_awaited()
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_and_answers_once_it_is_across(self, tmp_path, monkeypatch):
        monkeypatch.setenv("KS_READ_EXPENSES", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / "h2.duckdb")
        await store.connect()
        try:
            with patch("core.pg_expenses_read.backfilled",
                       new=AsyncMock(return_value=True)), \
                 patch("core.pg_expenses_read.fetch",
                       new=AsyncMock(return_value=[])) as fetch:
                await store.get_expense_summary(*W)
            fetch.assert_awaited()
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_body_that_does_not_read_it_is_not_gated(
        self, tmp_path, monkeypatch,
    ):
        """`manual_expenses` is replicated whole and the type dictionary is
        re-shipped every sync; neither has history to be missing."""
        monkeypatch.setenv("KS_READ_EXPENSES", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / "h3.duckdb")
        await store.connect()
        try:
            with patch("core.pg_expenses_read.backfilled",
                       new=AsyncMock(return_value=False)) as gate, \
                 patch("core.pg_expenses_read.fetch",
                       new=AsyncMock(return_value=[])) as fetch:
                await store.get_expense_types()
            fetch.assert_awaited()
            gate.assert_not_awaited()
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_latch_only_ever_closes_forward(self):
        """`backfilled_at` is set and never cleared, so a true answer is worth
        remembering for the life of the process."""
        from core import pg_expenses_read as r

        pool = MagicMock()
        conn = AsyncMock()
        conn.fetchval = AsyncMock(return_value=True)
        acquire = MagicMock()
        acquire.__aenter__ = AsyncMock(return_value=conn)
        acquire.__aexit__ = AsyncMock(return_value=False)
        pool.acquire = MagicMock(return_value=acquire)

        with patch("core.pg.get_pool", new=AsyncMock(return_value=pool)):
            assert await r.backfilled() is True
            assert await r.backfilled() is True
        assert conn.fetchval.await_count == 1, "a settled latch was re-read"

    @pytest.mark.asyncio
    async def test_an_unreachable_postgres_answers_no(self):
        """That read is not going to succeed either, so DuckDB serves."""
        from core import pg_expenses_read as r

        with patch("core.pg.get_pool",
                   new=AsyncMock(side_effect=RuntimeError("pg is down"))):
            assert await r.backfilled() is False


class TestItDoesNotTakeTheDuckDbLock:
    @pytest.mark.asyncio
    async def test_a_postgres_read_answers_while_the_store_lock_raises(
        self, tmp_path, monkeypatch,
    ):
        monkeypatch.setenv("KS_READ_EXPENSES", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / "ex.duckdb")
        await store.connect()
        try:
            def boom(*_a, **_k):
                raise AssertionError("the expense read reached for DuckDB")

            with patch.object(type(store), "connection", boom), \
                 patch("core.pg_expenses_read.fetch",
                       new=AsyncMock(return_value=[])) as fetch:
                await store.get_expense_types()
            assert "bronze.expense_types" in fetch.await_args.args[0]
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_the_duckdb_path_still_uses_the_store(self, tmp_path, monkeypatch):
        monkeypatch.delenv("KS_READ_EXPENSES", raising=False)
        store = DuckDBStore(db_path=tmp_path / "ex2.duckdb")
        await store.connect()
        try:
            with patch("core.pg_expenses_read.fetch",
                       new=AsyncMock(return_value=[])) as fetch:
                await store.get_expense_types()
            fetch.assert_not_awaited()
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_a_postgres_fault_falls_back_to_duckdb(self, tmp_path, monkeypatch):
        monkeypatch.setenv("KS_READ_EXPENSES", "postgres")
        monkeypatch.setenv("KS_PG_DSN", "postgresql://x/y")
        store = DuckDBStore(db_path=tmp_path / "ex3.duckdb")
        await store.connect()
        try:
            with patch("core.pg_expenses_read.fetch",
                       new=AsyncMock(side_effect=RuntimeError("pg is down"))):
                out = await store.get_expense_types()
            assert out == []
        finally:
            await store.close()
