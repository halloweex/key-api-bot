"""The chat tools' engine switch — the structure a two-engine test cannot see."""
from __future__ import annotations

import ast
import pathlib

import pytest

CHAT_TOOLS = pathlib.Path(__file__).resolve().parents[2] / "core" / "chat_tools.py"


def _functions():
    tree = ast.parse(CHAT_TOOLS.read_text(encoding="utf-8"))
    return {n.name: n for n in tree.body
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


class TestTheSwitch:
    def test_an_unknown_value_raises(self, monkeypatch):
        from core import pg_chat_read
        monkeypatch.setenv("KS_READ_CHAT", "postgre")
        with pytest.raises(ValueError):
            pg_chat_read.enabled()

    def test_default_is_duckdb(self, monkeypatch):
        from core import pg_chat_read
        monkeypatch.delenv("KS_READ_CHAT", raising=False)
        assert pg_chat_read.enabled() is False


class TestOneDoorToTheStore:
    def test_no_tool_opens_a_connection_of_its_own(self):
        """Walks every function in the module, not a list of the five: the next
        tool somebody adds with a raw `store.connection()` is the one a list
        would not name."""
        offenders = []
        for name, fn in _functions().items():
            if name == "_chat_run":
                continue
            for node in ast.walk(fn):
                if (isinstance(node, ast.Attribute) and node.attr == "connection"):
                    offenders.append(name)
        assert offenders == [], f"reads DuckDB around the switch: {offenders}"

    def test_the_router_has_no_fallback(self):
        router = _functions()["_chat_run"]
        assert not [n for n in ast.walk(router) if isinstance(n, ast.Try)], (
            "_chat_run catches — a Postgres failure must reach the model as an "
            "error, not come back as a frozen store's numbers")

    def test_every_gold_body_but_the_channels_keeps_the_roll_up(self):
        """Channels read the fine rows and must not carry the predicate; every
        other Gold read must, or Postgres counts each order twice."""
        texts = {}
        for name, fn in _functions().items():
            doc = ast.get_docstring(fn, clean=False)
            strings = [n.value for n in ast.walk(fn)
                       if isinstance(n, ast.Constant) and isinstance(n.value, str)
                       and n.value != doc]
            joined = "".join(strings)
            if "gold_daily_revenue}" in joined:
                texts[name] = joined
        assert set(texts) == {"_revenue_totals_sql", "_get_source_breakdown",
                              "_get_customer_insights"}, sorted(texts)
        for name, text in texts.items():
            has = "gold_revenue_rollup}" in text
            assert has is (name != "_get_source_breakdown"), name


class TestTheTopProductsTiebreak:
    def test_the_order_ends_on_an_id(self):
        """Structural on purpose. A two-engine test of a tie passes whenever
        both engines happen to emit the level rows in the same order, and on
        the fixture they do — removing the tiebreak left it green. What makes
        the cut deterministic is the ORDER BY itself, so that is what is read:
        its last key is an id, never a name, because the engines collate text
        differently."""
        fn = _functions()["_get_top_products"]
        sql = "".join(n.value for n in ast.walk(fn)
                      if isinstance(n, ast.Constant) and isinstance(n.value, str))
        order_by = sql[sql.index("ORDER BY"):sql.index("LIMIT")]
        assert order_by.rstrip().endswith("MIN(l.product_id)"), order_by
