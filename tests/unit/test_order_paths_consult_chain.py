"""Every DuckDB write of chain 3's tables asks the chain first (G6).

Under `KS_WRITE_ORDERS=postgres` the orders, their line items, their expenses
and the backfill-miss ledger are written to Postgres alone
(`core/pg_orders_write.py`), and a DuckDB statement against any of them writes
the store nobody reads. So every function in `core/`, `web/` and `scripts/`
whose own text writes one of the four DuckDB tables must evaluate the chain's
answer — `duckdb_store._orders_in_postgres`, `_refuse_if_orders_in_postgres`,
or `pg_orders_write.writes_postgres`/`mode` — or be reached only from functions
that do.

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

_ASK_NAMES = {"_orders_in_postgres", "_refuse_if_orders_in_postgres"}
_ASK_ATTRS = {"writes_postgres", "mode"}

# Writers that are not the live store's, each with the reason. The walk must
# find exactly these uncovered; a writer added to them is a decision.
EXEMPT = {
    ("core/migrations.py", "_m0007_order_products_drop_fk"):
        "a DuckDB schema migration, run once per file at connect, before any sync",
    ("core/migrations.py", "_m0008_expenses_drop_fk"):
        "a DuckDB schema migration, run once per file at connect, before any sync",
    ("core/migrations.py", "_m0011_order_products_id_bigint"):
        "a DuckDB schema migration, run once per file at connect, before any sync",
}


def _asks(fn) -> bool:
    for node in ast.walk(fn):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        if isinstance(f, ast.Name) and f.id in _ASK_NAMES:
            return True
        if (isinstance(f, ast.Attribute) and f.attr in _ASK_NAMES | _ASK_ATTRS
                and (f.attr in _ASK_NAMES
                     or (isinstance(f.value, ast.Name) and f.value.id == "pg_orders_write"))):
            return True
    return False


def _writes(fn) -> set:
    found = set()
    for node in ast.walk(fn):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            found |= {m.group(1).lower() for m in _WRITE.finditer(node.value)}
    return found


def _called(fn) -> set:
    out = set()
    for node in ast.walk(fn):
        if isinstance(node, ast.Call):
            f = node.func
            out.add(f.id if isinstance(f, ast.Name) else getattr(f, "attr", ""))
    return out


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
        # Callers by bare name — coarse on purpose: a name two modules share
        # makes both callers count, which can only make a writer harder to
        # call covered, never easier.
        self.callers = {}
        for key, fn in self.fns.items():
            for name in _called(fn):
                self.callers.setdefault(name, set()).add(key)

    def writers(self):
        return {key: tables for key, fn in self.fns.items()
                if (tables := _writes(fn))}

    def covered(self, key, seen=frozenset()) -> bool:
        if _asks(self.fns[key]):
            return True
        if key in seen:
            return False
        callers = self.callers.get(key[1], set()) - {key}
        return bool(callers) and all(self.covered(c, seen | {key}) for c in callers)


@pytest.fixture(scope="module")
def walk():
    return _Walk()


class TestEveryDuckDBOrderWriterAsks:
    def test_each_one_asks_or_is_reached_only_by_askers(self, walk):
        """Mutation: delete the chain-3 branch at the top of
        `web/routes/api/traffic._run_backfill_inner` — its `UPDATE orders` then
        restores comments into a DuckDB nobody reads while the chain writes
        Postgres."""
        uncovered = sorted(key for key in walk.writers()
                           if key not in EXEMPT and not walk.covered(key))
        assert not uncovered, (
            "writes chain 3's DuckDB tables without asking whether the chain "
            f"writes Postgres: {uncovered}")

    def test_the_exemptions_are_exactly_what_the_walk_finds(self, walk):
        found = {key for key in walk.writers() if not walk.covered(key)}
        assert found == set(EXEMPT), (found, set(EXEMPT))

    def test_the_walk_is_not_vacuous(self, walk):
        found = {name for (_rel, name) in walk.writers()}
        assert {"upsert_orders", "upsert_expenses_batch", "record_backfill_misses",
                "_run_backfill_inner", "backfill_utm"} <= found, found

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
        walk = _Walk({"web/x.py": source})
        assert walk.writers() == {("web/x.py", "restore"): {"orders"}}
        assert not walk.covered(("web/x.py", "restore"))

    def test_postgres_s_tables_are_not_duckdb_s(self):
        source = 'def f(c):\n    c.execute("UPDATE bronze.orders SET x = 1")\n'
        assert _Walk({"core/x.py": source}).writers() == {}

    def test_a_caller_that_asks_covers_a_helper(self):
        source = textwrap.dedent('''
            def _write(conn):
                conn.execute("INSERT OR REPLACE INTO order_backfill_misses VALUES (1)")

            def record(conn):
                if _orders_in_postgres():
                    return
                _write(conn)
        ''')
        walk = _Walk({"core/x.py": source})
        assert walk.covered(("core/x.py", "_write"))

    def test_an_unrelated_mode_call_is_not_an_answer(self):
        """`mode()` counts only on `pg_orders_write`: another chain's mode says
        nothing about where the orders go."""
        source = textwrap.dedent('''
            def f(conn):
                if pg_buyers_write.mode() != "duckdb":
                    return
                conn.execute("DELETE FROM expenses WHERE id = 1")
        ''')
        walk = _Walk({"core/x.py": source})
        assert not walk.covered(("core/x.py", "f"))
