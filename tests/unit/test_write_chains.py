"""Which tables have changed hands — one answer, found by walking, not listed.

Stage 4 moves writes chain by chain. When a chain writes Postgres, the hourly
shipper and the daily comparison must both stop touching its tables, and must
stop together. Both ask `core.write_chains.stood_down_tables()`.

The guard below walks `core/` rather than trusting `WRITE_CHAINS`: a guard that
names its subjects guards only the ones somebody remembered (the mirror-spec
guard saw 2 of 7 groups; the volume bound was written into one gate of two; the
transient-key exclusion named a retired key). A new write chain that forgets to
register itself would keep being full-replaced out of a frozen DuckDB — the
silent hourly rollback — and this is what makes that fail in CI instead.
"""
from __future__ import annotations

import ast
import inspect
import pathlib
import textwrap

import pytest

CORE = pathlib.Path(__file__).resolve().parents[2] / "core"


def _declares_a_write_chain(path: pathlib.Path) -> bool:
    """Top-level `CHAIN_TABLES = ...` and `def writes_postgres` — parsed, not
    imported, so walking the package has no side effects."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    names = set()
    for node in tree.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            names.update(t.id for t in targets if isinstance(t, ast.Name))
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(node.name)
    return {"CHAIN_TABLES", "writes_postgres"} <= names


class TestEveryWriteChainIsRegistered:
    def test_the_walk_finds_them_all_and_the_registry_holds_them_all(self):
        from core import write_chains

        found = {p.stem for p in CORE.glob("*.py") if _declares_a_write_chain(p)}
        registered = {m.__name__.rsplit(".", 1)[-1] for m in write_chains.WRITE_CHAINS}
        assert found, "the walk found no write chain — it is not looking"
        assert found == registered, (
            f"declared but not registered: {sorted(found - registered)}; "
            f"registered but not declaring: {sorted(registered - found)}")

    def test_the_walk_is_not_vacuous(self):
        """Both chains that exist today are found, so an empty walk cannot pass."""
        found = {p.stem for p in CORE.glob("*.py") if _declares_a_write_chain(p)}
        assert {"pg_inventory_write", "pg_expenses_write"} <= found


class TestTheShipperAndTheComparisonAskOneAnswer:
    def test_both_sites_call_the_registry_and_neither_spells_a_chain(self):
        from core import mirror_reconciliation, pg_operational

        for fn in (pg_operational.replicate_operational,
                   mirror_reconciliation.reconcile_operational):
            tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
            calls = {n.func.id for n in ast.walk(tree)
                     if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
            # The checked form since DN-01: it never raises on a flag typo and
            # hands the error out to be recorded and reported.
            assert "stood_down_tables_checked" in calls, fn.__name__
            assert "writes_postgres" not in calls, (
                f"{fn.__name__} asks a single chain again — the rule would have two homes")


@pytest.fixture
def flags(monkeypatch):
    for env in ("KS_WRITE_INVENTORY", "KS_WRITE_EXPENSES"):
        monkeypatch.delenv(env, raising=False)
    return monkeypatch


class TestWhatStandsDown:
    def test_nothing_while_every_chain_writes_duckdb(self, flags):
        from core.write_chains import stood_down_tables
        assert stood_down_tables() == frozenset()

    def test_only_the_expenses_table_when_only_chain_8_is_on(self, flags):
        from core.write_chains import stood_down_tables
        flags.setenv("KS_WRITE_EXPENSES", "postgres")
        assert stood_down_tables() == frozenset({"app.manual_expenses"})

    def test_the_union_when_both_are_on(self, flags):
        from core import pg_inventory_write
        from core.write_chains import stood_down_tables
        flags.setenv("KS_WRITE_EXPENSES", "postgres")
        flags.setenv("KS_WRITE_INVENTORY", "postgres")
        assert stood_down_tables() == (
            frozenset(pg_inventory_write.CHAIN_TABLES) | {"app.manual_expenses"})

    def test_an_unknown_expenses_value_raises(self, flags):
        from core.pg_expenses_write import writes_postgres
        flags.setenv("KS_WRITE_EXPENSES", "postgre")
        with pytest.raises(RuntimeError):
            writes_postgres()

    def test_the_expenses_table_is_one_the_shipper_actually_replaces(self):
        """Standing down a table the shipper never touched would be a no-op that
        reads as a guarantee."""
        from core.pg_expenses_write import CHAIN_TABLES
        from core.pg_operational import _FULL_REPLACE
        assert set(CHAIN_TABLES) <= {pg for pg, _d, _c, _o in _FULL_REPLACE}
