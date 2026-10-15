"""Stage 5 (PR-9): the bot's process never loads DuckDB.

The week of silence is judged on web, because web is the only process that
ever opened the file — and that holds only while the bot does not. It does
not today, and nothing said so: the design read it once by importing
`bot.main` (design 2.8). It is pinned here twice, because each half misses
what the other sees.

- **What the bot imports.** In a fresh interpreter, every module under
  `bot/`, then every repository module any line of `bot/` imports —
  function-level imports included, which is where most of the bot's
  `core.*` arrive. Neither the driver nor `core.duckdb_store` may be loaded.
- **What the bot calls.** An import inside a `core` function arrives only
  when that function runs — `core.permissions` imports the store inside the
  three async permission readers web uses, and the bot imports
  `core.permissions` for its names. So the calls are walked too
  (`test_read_fallback_consumers.build_graph`, over `bot/` and `core/`): no
  function in `bot/` may reach, through any call the walk resolves, one that
  imports DuckDB or the store, or asks for the store (`get_store`,
  `DuckDBStore(...)`, `get_sync_service`, `open_file`). A call the walk
  resolves by its name alone is left out — `self._thread.start()` is not
  `BackgroundScheduler.start` — and so is anything the walk cannot read: a
  `getattr`, a table of callables.

Web runs bot code too — the bot store behind the sign-ins and the access
requests, `send_admin_message` behind the alerts. That is web's process, and
the stage 5 sweep (`test_duckdb_off_sweep.py`) runs those paths there.
"""
from __future__ import annotations

import ast
import json
import pathlib
import subprocess
import sys
from typing import Dict, Set

REPO = pathlib.Path(__file__).resolve().parents[2]

# What the bot's process must never hold: the driver, and the store that is
# the one reach for it (`core/duckdb_switch.py`).
FORBIDDEN = ("duckdb", "core.duckdb_store")
# A function that asks for the store or the file by these names.
STORE_CALLS = {"get_store", "DuckDBStore", "get_sync_service", "open_file"}


def _module_name(path: pathlib.Path) -> str:
    rel = path.relative_to(REPO).with_suffix("")
    parts = rel.parts[:-1] if rel.name == "__init__" else rel.parts
    return ".".join(parts)


def _repository_module(dotted: str) -> bool:
    rel = dotted.replace(".", "/")
    return (REPO / f"{rel}.py").is_file() or (REPO / rel / "__init__.py").is_file()


def bot_imports() -> Set[str]:
    """Every module under `bot/`, and every repository module any line of
    `bot/` imports, at the top or inside a function."""
    found: Set[str] = set()
    for path in sorted((REPO / "bot").rglob("*.py")):
        found.add(_module_name(path))
        package = list(path.relative_to(REPO).parent.parts)
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                base = package[:len(package) - (node.level - 1)] if node.level else []
                module = ".".join(base + ([node.module] if node.module else []))
                names = [module] + [f"{module}.{a.name}" for a in node.names]
            else:
                continue
            found.update(n for n in names if n and _repository_module(n))
    return found


_PROBE = """
import importlib, json, sys
modules = json.loads(sys.argv[1])
for name in modules:
    importlib.import_module(name)
loaded = sorted(m for m in sys.modules if m in {forbidden} or
                any(m.startswith(f + ".") for f in {forbidden}))
print(json.dumps(loaded))
"""


class TestWhatTheBotImports:
    def test_nothing_the_bot_imports_loads_duckdb(self):
        """A fresh interpreter, so nothing a test before this one loaded is
        mistaken for the bot's. Mutation: `import duckdb` at the top of any
        module the bot imports — this fails naming it."""
        modules = sorted(bot_imports())
        probe = _PROBE.replace("{forbidden}", repr(set(FORBIDDEN)))
        proc = subprocess.run(
            [sys.executable, "-c", probe, json.dumps(modules)],
            cwd=REPO, capture_output=True, text=True, timeout=300)
        assert proc.returncode == 0, proc.stderr[-3000:]
        loaded = json.loads(proc.stdout.strip().splitlines()[-1])
        assert loaded == [], f"the bot's imports load {loaded}"

    def test_the_imports_are_found(self):
        """Non-vacuity: the bot's own modules, and the `core` modules it
        imports only inside functions."""
        modules = bot_imports()
        assert {"bot.main", "bot.canary", "bot.store_postgres"} <= modules
        # Imported only inside functions of `bot/`.
        assert {"core.bot_store", "core.alert_archive", "core.dashboard_access"} <= modules


def _store_touches(fn) -> bool:
    """`fn`'s own body imports DuckDB or the store, or asks for the store."""
    from tests.unit.test_read_fallback_consumers import _own

    for node in _own(fn.node):
        if isinstance(node, ast.Import):
            if any(a.name.split(".")[0] == "duckdb" or a.name == "core.duckdb_store"
                   for a in node.names):
                return True
        elif isinstance(node, ast.ImportFrom):
            if node.module and (node.module.split(".")[0] == "duckdb"
                                or node.module == "core.duckdb_store"):
                return True
        elif isinstance(node, ast.Call):
            f = node.func
            name = f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", None)
            if name in STORE_CALLS:
                return True
    return False


def store_reaching(graph) -> Dict[object, object]:
    """Every function reaching one that touches the store, through calls the
    walk resolves by more than a name — mapped to the next step towards it."""
    touching = {fn for fn in graph.functions if _store_touches(fn)}
    edges = {fn: set().union(*(targets for call, targets in found
                               if call not in graph.guessed)) if found else set()
             for fn, found in graph.sites.items()}
    toward: Dict[object, object] = {fn: None for fn in touching}
    changed = True
    while changed:
        changed = False
        for fn, targets in edges.items():
            if fn in toward:
                continue
            hit = next((t for t in targets if t in toward), None)
            if hit is not None:
                toward[fn] = hit
                changed = True
    return toward


class TestWhatTheBotCalls:
    def test_no_bot_function_reaches_the_store(self):
        from tests.unit.test_read_fallback_consumers import build_graph

        graph = build_graph(("bot", "core"))
        toward = store_reaching(graph)

        def chain(fn):
            out = []
            while fn is not None:
                out.append(fn.key)
                fn = toward[fn]
            return " -> ".join(out)

        bad = sorted((fn for fn in toward if fn.rel.startswith("bot/")),
                     key=lambda fn: fn.key)
        assert not bad, "bot code reaching the store:\n" + "\n".join(chain(fn) for fn in bad)

    def test_the_walk_sees_the_store_where_it_is(self):
        """Non-vacuity: the walk finds the paths it exists to find — the
        permission readers the bot imports its names beside, and a job that
        asks for the store — so an empty answer above is the bot's, not the
        walk's."""
        from tests.unit.test_read_fallback_consumers import build_graph

        keys = {fn.key for fn in store_reaching(build_graph(("bot", "core")))}
        assert "core/permissions.py:get_permissions_for_role_async" in keys
        assert "core/scheduler.py:BackgroundScheduler._run_dq_digest" in keys
