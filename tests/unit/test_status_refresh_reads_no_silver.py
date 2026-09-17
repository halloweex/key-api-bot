"""The 05:15 status refresh does not read DuckDB's Silver back.

It used to: after rewriting the last 30 days of orders it compared a sample of
twenty return orders against `silver_orders` and wrote the verdict to a log
line and to `stats["verification_failed"]`, which nothing read. DuckDB is on
its way out, and `dq_reconciliation` already compares every order's status
against KeyCRM at 05:30 in all three stores, filing a moved status as
STATUS_DRIFT.

Parsed rather than grepped, so a comment explaining the removal cannot fail the
test and a table name tucked into an f-string cannot pass it.
"""
from __future__ import annotations

import ast
from pathlib import Path

SOURCE = Path("core/sync_service.py")


def _refresh_function() -> ast.AsyncFunctionDef:
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if (isinstance(node, ast.ClassDef) and node.name == "SyncService"):
            for item in node.body:
                if (isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                        and item.name == "refresh_order_statuses"):
                    return item
    raise AssertionError("SyncService.refresh_order_statuses is gone; move this guard with it")


def _string_constants(fn: ast.AST) -> list[str]:
    # `ast.walk` reaches the literal parts of an f-string too, as Constants
    # inside the JoinedStr.
    return [n.value for n in ast.walk(fn)
            if isinstance(n, ast.Constant) and isinstance(n.value, str)]


def test_the_scan_sees_the_functions_strings():
    """A guard on the guard: a walk that found nothing would pass below."""
    strings = _string_constants(_refresh_function())
    assert any("Refreshing order statuses" in s for s in strings)


def test_the_status_refresh_names_no_silver_orders():
    offenders = [s for s in _string_constants(_refresh_function())
                 if "silver_orders" in s]
    assert not offenders, (
        f"refresh_order_statuses reads DuckDB Silver again: {offenders!r}"
    )


def test_the_status_refresh_opens_no_store_connection():
    """Its writes go through store methods; a raw connection here is a read
    somebody added back, whatever table it names."""
    calls = [n for n in ast.walk(_refresh_function())
             if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Attribute)
             and n.func.attr == "connection"]
    assert not calls, "refresh_order_statuses opens the store's connection itself"
