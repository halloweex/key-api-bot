"""Chain 1's pre-flip hardening (DN-24), the parts that need no database.

KS_WRITE_INVENTORY stays off; every guard here is about what happens the day
it is switched on. The same properties against a real Postgres are in
`tests/integration/test_inventory_preflip.py`.
"""
from __future__ import annotations

import ast
import inspect
import pathlib

from core import pg_inventory_write

# The tables whose rebuild or daily photograph two callers can race on. The
# stock upsert and the offer upsert are not here: both run only in the stock
# step, under the scheduler's heavy lock, and nothing else calls them.
_REBUILT_OR_PHOTOGRAPHED = (
    "app.sku_inventory_status",
    "app.inventory_sku_history",
    "app.inventory_history",
)


def _module_tree() -> ast.Module:
    return ast.parse(pathlib.Path(inspect.getfile(pg_inventory_write)).read_text(
        encoding="utf-8"))


def _writes_one_of_them(node: ast.AsyncFunctionDef) -> bool:
    """Whether the function writes a rebuilt or photographed table: a literal
    INSERT or DELETE naming it, or the shared rebuild body, which is composed
    elsewhere and never appears here as text."""
    for n in ast.walk(node):
        if isinstance(n, ast.Constant) and isinstance(n.value, str):
            text = n.value.strip().upper()
            if text.startswith(("INSERT", "DELETE")) and any(
                    t.upper() in text for t in _REBUILT_OR_PHOTOGRAPHED):
                return True
        if isinstance(n, ast.Call) and getattr(n.func, "id", "") == "sku_status_rebuild_select":
            return True
    return False


def _transactions(node: ast.AsyncFunctionDef):
    """Every `async with <conn>.transaction():` body in the function."""
    for n in ast.walk(node):
        if isinstance(n, ast.AsyncWith) and any(
                isinstance(item.context_expr, ast.Call)
                and getattr(item.context_expr.func, "attr", "") == "transaction"
                for item in n.items):
            yield n


def _first_await(body) -> str:
    first = body[0]
    if isinstance(first, ast.Expr) and isinstance(first.value, ast.Await):
        call = first.value.value
        if isinstance(call, ast.Call):
            return getattr(call.func, "id", getattr(call.func, "attr", ""))
    return ""


def _rebuilders() -> dict:
    return {node.name: node for node in _module_tree().body
            if isinstance(node, ast.AsyncFunctionDef) and _writes_one_of_them(node)}


class TestEveryRebuildAndSnapshotTakesTheChainLock:
    def test_the_walk_finds_the_three_that_exist(self):
        # Not vacuous: a walk that found nothing would pass the test below.
        assert set(_rebuilders()) == {
            "rebuild_sku_inventory_status",
            "record_sku_inventory_snapshot",
            "record_inventory_snapshot",
        }

    def test_the_lock_is_the_first_statement_of_each_transaction(self):
        """First, before the owner rows and before any read: the second of two
        callers must read what the first committed. Taken after the
        `first_seen_at` carry-forward, the rebuild would still copy a state
        the other one was about to replace."""
        for name, node in _rebuilders().items():
            blocks = list(_transactions(node))
            assert blocks, f"{name} writes outside a transaction"
            for block in blocks:
                assert _first_await(block.body) == "_chain_lock", (
                    f"{name} does not take the chain-1 advisory lock first")

    def test_it_is_a_transaction_lock_on_the_chains_own_key(self):
        """`_xact_`, so COMMIT or ROLLBACK releases it and a writer that dies
        cannot leave it held; and a bigint Postgres accepts."""
        source = inspect.getsource(pg_inventory_write._chain_lock)
        assert "pg_advisory_xact_lock(" in source and "CHAIN_LOCK_KEY" in source
        assert 0 < pg_inventory_write.CHAIN_LOCK_KEY < 2 ** 63
        assert pg_inventory_write.CHAIN_LOCK_KEY & 0xFFFF_FFFF == 1   # chain 1
