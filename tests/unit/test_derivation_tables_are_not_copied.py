"""`meta.derivation_*` is safe only because nothing copies or compares it.

The signal lives in `meta` precisely so the hourly full replace out of DuckDB
cannot clear it (revision 0033, and 0032 before it). A spec in the shipper or the
comparison naming these tables is the first step back to that.
"""
from __future__ import annotations

import ast
import pathlib

CORE = pathlib.Path(__file__).resolve().parents[2] / "core"


def test_no_copy_or_comparison_names_the_derivation_tables():
    for name in ("pg_operational.py", "mirror_reconciliation.py", "pg_replication.py"):
        tree = ast.parse((CORE / name).read_text(encoding="utf-8"))
        docstrings = {id(n.value) for n in ast.walk(tree)
                      if isinstance(n, ast.Expr) and isinstance(n.value, ast.Constant)}
        named = [n.value for n in ast.walk(tree)
                 if isinstance(n, ast.Constant) and isinstance(n.value, str)
                 and id(n) not in docstrings and "derivation_" in n.value]
        assert named == [], f"{name} names meta.derivation_*: {named}"
