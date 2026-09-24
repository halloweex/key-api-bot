"""DN-05b: the cached modes are configured before anything can write.

`KS_PG_DERIVE` was read in `BackgroundScheduler.start`, and web's startup runs
the boot sync before it starts the scheduler — so the boot's orders landed in
Postgres while `owns()` still answered False, and every mark was skipped in
silence. What is pinned here is the order of calls at each entry point, read
out of the code, because no running test could see a boot.
"""
from __future__ import annotations

import ast
import pathlib

import pytest

from core import pg_derivation
from core.runtime_modes import configure_modes

REPO = pathlib.Path(__file__).resolve().parents[2]


def _parse(relative: str) -> ast.Module:
    return ast.parse((REPO / relative).read_text(encoding="utf-8"))


def _called_name(call: ast.Call) -> "str | None":
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _calls(node: ast.AST, name: str) -> list:
    return [n for n in ast.walk(node)
            if isinstance(n, ast.Call) and _called_name(n) == name]


@pytest.fixture
def restore_mode():
    before = (pg_derivation._mode, pg_derivation._mode_error)
    yield
    pg_derivation._mode, pg_derivation._mode_error = before


class TestConfigureModes:
    def test_it_sets_the_derivation_mode_and_a_second_call_changes_nothing(
        self, monkeypatch, restore_mode,
    ):
        monkeypatch.setenv(pg_derivation.ENV, "own")
        first = configure_modes()
        second = configure_modes()
        assert first == second
        assert first[pg_derivation.ENV] == "own"
        assert pg_derivation.owns() and pg_derivation.mode_error() is None

    def test_unset_is_piggyback(self, monkeypatch, restore_mode):
        monkeypatch.setenv(pg_derivation.ENV, "own")
        configure_modes()
        monkeypatch.delenv(pg_derivation.ENV)
        assert configure_modes()[pg_derivation.ENV] == "piggyback"
        assert not pg_derivation.owns()

    def test_it_reaches_every_cached_mode_in_core(self):
        """Walks `core/` for modules defining `configure_mode` rather than
        trusting a list: a second cached mode that the entry points never
        configure is this bug again, one mode later."""
        defining = set()
        for path in (REPO / "core").rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            if any(isinstance(n, ast.FunctionDef) and n.name == "configure_mode"
                   for n in tree.body):
                defining.add(".".join(path.relative_to(REPO).with_suffix("").parts))
        assert "core.pg_derivation" in defining, "the walk is not looking"

        tree = _parse("core/runtime_modes.py")
        imported = {alias.asname or alias.name: f"{n.module}.{alias.name}"
                    for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)
                    for alias in n.names}
        reached = {imported.get(call.func.value.id)
                   for call in _calls(tree, "configure_mode")
                   if isinstance(call.func, ast.Attribute)
                   and isinstance(call.func.value, ast.Name)}
        assert defining <= reached, sorted(defining - reached)


class TestWebConfiguresBeforeTheBootSync:
    def _startup(self) -> ast.AsyncFunctionDef:
        tree = _parse("web/main.py")
        found = [n for n in ast.walk(tree)
                 if isinstance(n, ast.AsyncFunctionDef) and n.name == "startup_event"]
        assert len(found) == 1, "web's startup handler moved — update this walk"
        return found[0]

    def test_configure_modes_runs_before_init_and_sync(self):
        startup = self._startup()
        configure = _calls(startup, "configure_modes")
        sync = _calls(startup, "init_and_sync")
        assert sync, "the boot sync moved — this test no longer guards it"
        assert configure, "web's startup never configures the cached modes"
        assert min(c.lineno for c in configure) < min(s.lineno for s in sync)

    def test_no_branch_can_skip_it(self):
        """A plain statement of the handler's own body, not inside a `try` or
        an `if`: a mode that is configured only on some boots is the hole."""
        startup = self._startup()
        assert any(isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call)
                   and _called_name(stmt.value) == "configure_modes"
                   for stmt in startup.body)


class TestTheSchedulerStillConfigures:
    def test_start_calls_configure_modes(self):
        """Idempotent, so web's earlier call costs nothing — and a scheduler
        started anywhere else stays correct."""
        tree = _parse("core/scheduler.py")
        start = [n for n in ast.walk(tree)
                 if isinstance(n, ast.AsyncFunctionDef) and n.name == "start"]
        assert start and any(_calls(fn, "configure_modes") for fn in start)


# What reaches a writer that raises the derivation signal. The sync service is
# how scripts reach `upsert_orders`; the rest are named so a script that goes
# straight to a Postgres writer is caught too.
WRITER_MODULES = frozenset({
    "core.sync_service", "core.pg_landing", "core.pg_backfill",
    "core.pg_buyers", "core.pg_replication",
})
WRITER_CALLS = frozenset({
    "upsert_orders", "upsert_managers", "upsert_buyers", "write_orders",
    "mirror_orders", "mirror_buyers", "backfill_orders", "backfill_buyers",
    "replicate_managers", "ship_orders_by_id",
})


def _enclosing_functions(tree: ast.Module) -> dict:
    """Call node -> the innermost function it sits in (None at module level)."""
    owner: dict = {}

    def visit(node, fn):
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)):
                visit(child, child)
            else:
                if isinstance(child, ast.Call):
                    owner[child] = fn
                visit(child, fn)

    visit(tree, None)
    return owner


class TestScriptsConfigureBeforeTheyWrite:
    def _scripts(self):
        """`{path: (tree, writer names)}` for every script that reaches a writer."""
        out = {}
        for path in sorted((REPO / "scripts").glob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            names = set(WRITER_CALLS)
            imports_writer = False
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom) and node.module in WRITER_MODULES:
                    imports_writer = True
                    names |= {a.asname or a.name for a in node.names}
                elif isinstance(node, ast.Import) and any(
                        a.name in WRITER_MODULES for a in node.names):
                    imports_writer = True
            calls_writer = any(_called_name(n) in WRITER_CALLS for n in ast.walk(tree)
                               if isinstance(n, ast.Call))
            if imports_writer or calls_writer:
                out[path.name] = (tree, names)
        return out

    def test_the_walk_finds_the_scripts_that_write(self):
        """Two today, and the second is why the walk was written rather than a
        list typed out. `force_resync.py` reaches `upsert_orders` through the
        sync service. `backfill_utm.py` was named here as the near miss — it
        rewrote `manager_comment` in DuckDB with a plain `UPDATE` and reached
        Postgres only through `ship_after_reparse`, whose table no derivation
        reads. DN-17 gave it `ship_orders_by_id`, so it writes `bronze.orders`
        now, and this walk found it on the first run rather than a reviewer
        finding it later."""
        found = self._scripts()
        assert "force_resync.py" in found, sorted(found)
        assert "backfill_utm.py" in found, sorted(found)

    def test_every_one_configures_the_modes_before_its_first_write(self):
        for name, (tree, writer_names) in self._scripts().items():
            owner = _enclosing_functions(tree)
            writes = [c for c in owner if _called_name(c) in writer_names]
            assert writes, f"{name}: imports a writer module but the walk sees no call"
            configures = [c for c in owner if _called_name(c) == "configure_modes"]
            for write in writes:
                before = [c for c in configures
                          if owner[c] is owner[write] and c.lineno < write.lineno]
                assert before, (
                    f"{name}:{write.lineno} calls {_called_name(write)} without "
                    "configure_modes() earlier in the same function — its writes "
                    "would land in Postgres unmarked under KS_PG_DERIVE=own"
                )
