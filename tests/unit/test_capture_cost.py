"""The cost capture's shape, read off the code (no database).

`tests/integration/test_capture_cost_pg.py` runs it on a server; this pins
what a server run cannot show by itself: which columns the statement writes,
where in chain 1's photograph it runs, and that no statement anywhere ever
changes a cost row once written.
"""
from __future__ import annotations

import ast
import inspect
import re
from pathlib import Path

from core import pg_inventory_write
from tests import migration_replay as replay

REPO = Path(__file__).resolve().parents[2]
TABLE = "app.offer_cost_daily"


def _flat(sql: str) -> str:
    return " ".join(sql.lower().split())


def _table_columns() -> list:
    """`app.offer_cost_daily`'s columns, in order, as revision 0036 creates it."""
    rev = replay.load(replay.VERSIONS / "0036_analytics_captures.py")
    ddl = next(s for s in rev.STATEMENTS if f"create table if not exists {TABLE} (" in _flat(s))
    body = ddl[ddl.index(f"{TABLE} (") + len(TABLE) + 2:]
    lines = [line.strip() for line in body.splitlines()]
    return [line.split()[0] for line in lines
            if line and not line.startswith(("PRIMARY KEY", ")", "--"))]


class TestTheStatement:
    def test_it_fills_every_column_of_the_table_from_the_stock_table(self):
        """Kills: a column the revision has and the capture leaves NULL, or a
        capture reading the status table instead of `offer_stocks`, whose
        purchase price is the one `/margin` costs by."""
        flat = _flat(pg_inventory_write._COST_CAPTURE_SQL)
        head = re.match(rf"insert into {re.escape(TABLE)} \(([^)]*)\) select ", flat)
        assert head, flat
        assert [c.strip() for c in head.group(1).split(",")] == _table_columns()
        assert " from bronze.offer_stocks s " in flat

    def test_a_rerun_of_the_day_keeps_the_first_cost(self):
        """DO NOTHING on the table's key, never DO UPDATE (WRITER-cost.md)."""
        flat = _flat(pg_inventory_write._COST_CAPTURE_SQL)
        assert flat.endswith("on conflict (date, offer_id) do nothing")
        assert "do update" not in flat


def _fn(tree, name):
    return next(n for n in ast.walk(tree)
                if isinstance(n, ast.AsyncFunctionDef) and n.name == name)


def _capture_calls(node):
    return [n for n in ast.walk(node) if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name) and n.func.id == "capture_in_transaction"]


class TestWhereItRuns:
    def test_once_in_the_photographs_transaction_after_its_guard_with_its_day(self):
        """Kills: the capture outside the transaction (the helper refuses
        it), before the "already taken" guard (it would run every hour), in
        another writer of the chain, or dated by anything but `day`."""
        tree = ast.parse(inspect.getsource(pg_inventory_write))
        everywhere = _capture_calls(tree)
        snapshot = _fn(tree, "record_sku_inventory_snapshot")
        assert len(everywhere) == 1 and _capture_calls(snapshot) == everywhere
        call = everywhere[0]

        block = next(n for n in ast.walk(snapshot) if isinstance(n, ast.AsyncWith)
                     and any(isinstance(i.context_expr, ast.Call)
                             and isinstance(i.context_expr.func, ast.Attribute)
                             and i.context_expr.func.attr == "transaction"
                             for i in n.items))
        assert call in [n for n in ast.walk(block)]

        guard = next(n for n in block.body if isinstance(n, ast.If)
                     and any(isinstance(r, ast.Return) for r in n.body))
        photo = next(n for n in block.body if "inventory_sku_history (" in _flat(
            " ".join(c.value for c in ast.walk(n)
                     if isinstance(c, ast.Constant) and isinstance(c.value, str))))
        at = next(i for i, n in enumerate(block.body) if call in list(ast.walk(n)))
        assert block.body.index(guard) < block.body.index(photo) < at

        conn, table, sql, day = call.args
        assert isinstance(table, ast.Constant) and table.value == TABLE
        assert isinstance(sql, ast.Name) and sql.id == "_COST_CAPTURE_SQL"
        assert isinstance(day, ast.Name) and day.id == "day"


def _sql_literals(path: Path):
    """Every string literal in a module that is not a docstring — where SQL
    lives, and not the prose explaining why a statement never exists."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            first = node.body[0] if node.body else None
            if (isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant)
                    and isinstance(first.value.value, str)):
                docstrings.add(id(first.value))
    for node in ast.walk(tree):
        if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and id(node) not in docstrings):
            yield node.value


class TestAppendOnly:
    def test_no_statement_anywhere_changes_or_removes_a_cost_row(self):
        """The only cost history there will be: a row once written is what
        the day's photograph found. Pinned by parsing the trees the analytics
        code lives in, not by a REVOKE (`ks_app` owns the table and could
        grant itself back what it revoked)."""
        verbs = (f"update {TABLE}", f"delete from {TABLE}", f"truncate {TABLE}",
                 f"truncate table {TABLE}", f"drop table {TABLE}",
                 f"drop table if exists {TABLE}")
        offenders = []
        for tree in ("core", "web", "scripts", "bot", "deploy"):
            for path in sorted((REPO / tree).rglob("*.py")):
                for literal in _sql_literals(path):
                    flat = _flat(literal)
                    if any(v in flat for v in verbs):
                        offenders.append(str(path.relative_to(REPO)))
        assert not offenders, f"append-only violated in: {sorted(set(offenders))}"
