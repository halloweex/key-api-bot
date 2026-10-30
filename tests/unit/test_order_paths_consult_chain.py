"""No DuckDB statement writes chain 3's tables any more (G6, after S14-09).

Chain 3 writes the orders, their line items, their expenses and the
backfill-miss ledger to Postgres alone (`core/pg_orders_write.py`). Until step
14's S14-09 this walk required every DuckDB write of those four tables to ask
the chain first — `duckdb_store._orders_in_postgres`,
`_refuse_if_orders_in_postgres`, or `pg_orders_write.writes_postgres`/`mode` —
because a DuckDB statement against any of them writes the store nobody reads.
S14-09 deleted the writers themselves (`upsert_orders`,
`upsert_expenses_batch`, the DuckDB halves of `record_backfill_misses` and of
the two comment backfills) with the answers they asked, so what is held now is
the stronger statement: the walk finds no such writer at all, but DuckDB's own
schema migrations, which run once per file at connect and go with the store
(S14-18).

Walked, not listed: the two comment backfills were writers of `orders` that
went round `upsert_orders` entirely, and a list written from memory is how
they would have been forgotten. Each test names the mutation it fails on.
"""
from __future__ import annotations

import ast
import pathlib
import re
import textwrap

import pytest

REPO = pathlib.Path(__file__).resolve().parents[2]

# A DuckDB write of one of the four tables, by its statement: not
# `bronze.orders` (Postgres's) — the table name stands alone.
_WRITE = re.compile(
    r"(?<![\w.])(?:INSERT\s+(?:OR\s+REPLACE\s+)?INTO|UPDATE|DELETE\s+FROM)\s+"
    r"(orders|order_products|expenses|order_backfill_misses)\b",
    re.IGNORECASE,
)

# The writers left, each with the reason. The walk must find exactly these; a
# writer added to them is a decision.
EXEMPT = {
    ("core/migrations.py", "_m0007_order_products_drop_fk"):
        "a DuckDB schema migration, run once per file at connect, before any sync",
    ("core/migrations.py", "_m0008_expenses_drop_fk"):
        "a DuckDB schema migration, run once per file at connect, before any sync",
    ("core/migrations.py", "_m0011_order_products_id_bigint"):
        "a DuckDB schema migration, run once per file at connect, before any sync",
}


def _writes(fn) -> set:
    found = set()
    for node in ast.walk(fn):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            found |= {m.group(1).lower() for m in _WRITE.finditer(node.value)}
    return found


class _Walk:
    def __init__(self, sources=None):
        self.fns = {}
        if sources is None:
            sources = {}
            for folder in ("core", "web", "scripts"):
                for path in sorted((REPO / folder).rglob("*.py")):
                    sources[path.relative_to(REPO).as_posix()] = path.read_text(
                        encoding="utf-8")
        for rel, text in sources.items():
            for node in ast.walk(ast.parse(text)):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    self.fns[(rel, node.name)] = node

    def writers(self):
        return {key: tables for key, fn in self.fns.items()
                if (tables := _writes(fn))}


@pytest.fixture(scope="module")
def walk():
    return _Walk()


class TestNoDuckDBOrderWriterIsLeft:
    def test_the_only_writers_left_are_duckdb_s_own_migrations(self, walk):
        """Mutation: put back a DuckDB `UPDATE orders` in the traffic route's
        comment backfill, or `upsert_orders` in the store — a write of the
        store nobody reads, with nothing left to ask whether it should."""
        assert set(walk.writers()) == set(EXEMPT), (
            "a DuckDB statement writes chain 3's tables: "
            f"{sorted(set(walk.writers()) - set(EXEMPT))}")

    @pytest.mark.parametrize("name", ["upsert_orders", "upsert_expenses_batch"])
    def test_the_store_s_order_writers_are_gone(self, walk, name):
        assert not [key for key in walk.fns if key[1] == name], name

    def test_the_dead_per_order_expense_writer_is_gone(self, walk):
        """`upsert_expenses` had no caller anywhere and parsed the payload
        inline instead of through `landing_rows.expense_rows`; chain 3 deleted
        it rather than route it."""
        assert ("core/repositories/expenses.py", "upsert_expenses") not in walk.fns


class TestTheWalkSeesWhatItMust:
    def test_an_fstring_write_is_a_write(self):
        source = textwrap.dedent('''
            def restore(conn, ids):
                conn.execute(f"UPDATE orders SET manager_comment = ? WHERE id IN ({ids})")
        ''')
        assert _Walk({"web/x.py": source}).writers() == {("web/x.py", "restore"): {"orders"}}

    def test_postgres_s_tables_are_not_duckdb_s(self):
        source = 'def f(c):\n    c.execute("UPDATE bronze.orders SET x = 1")\n'
        assert _Walk({"core/x.py": source}).writers() == {}

    def test_each_of_the_four_tables_is_seen(self):
        source = textwrap.dedent('''
            def a(c):
                c.execute("INSERT OR REPLACE INTO order_backfill_misses VALUES (1)")

            def b(c):
                c.execute("DELETE FROM order_products WHERE order_id = 1")

            def d(c):
                c.execute("INSERT INTO expenses VALUES (1)")
        ''')
        assert _Walk({"core/x.py": source}).writers() == {
            ("core/x.py", "a"): {"order_backfill_misses"},
            ("core/x.py", "b"): {"order_products"},
            ("core/x.py", "d"): {"expenses"}}
