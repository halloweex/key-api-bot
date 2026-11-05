"""Step 14's S14-19: the images carry no DuckDB, and nothing they run asks for it.

`requirements.lock` — what `Dockerfile` and `Dockerfile.web` install — has no
`duckdb` since S14-19, so the bot's and web's images hold no driver. That is
safe only while no module either of them can run imports it, at the top or
inside a function, and two halves hold it, because each misses what the
other sees:

- **The walk.** Every Python file of the code trees (`core/`, `web/`, `bot/`,
  `scripts/`, `deploy/`, `migrations/`) is parsed, and no import of the
  driver may be left in any form: `import duckdb` (aliased, dotted, beside
  other names), `from duckdb …`, `importlib.import_module("duckdb")` or
  `__import__("duckdb")` (by any name the function is bound to, the module
  name a literal or a module-level constant), `sys.modules["duckdb"]` and its
  `.get`/`.pop`/`.setdefault`, and any of these inside a string that is a
  Python program of its own (`python -c "import duckdb"`). The import test
  below sees only what an import runs; `test_duckdb_switch.py` and
  `test_duckdb_open_guard.py`, which read the lazy forms, went in S14-18.
  The exceptions are a frozen list of host tools in `deploy/`, which run on
  the pinned `rollback-step14` image and never in these images
  (`deploy/archive_image.sh`, `tests/unit/test_archive_image.py`); each must
  still import what it is excused for, so the list cannot outlive its reason.
  `compact_duckdb` counts as the driver: it is DuckDB's compaction, deleted
  from this tree in S14-18, which the pinned image carries and which imports
  `duckdb` at its top.
- **The import.** In a fresh interpreter in which `duckdb` cannot be found —
  the images' state, made so whatever this environment installed, since CI
  and the VPS gate install `requirements-dev.lock`, which keeps the driver
  for the host tools' tests — every module under `core/`, `web/` and `bot/`
  and every script under `scripts/` imports, and the driver is not loaded.

What neither sees: a module name computed at run time, or handed to
`import_module` from anything but a literal or a module-level string.
"""
from __future__ import annotations

import ast
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, Iterator, List, Optional

import pytest

REPO = Path(__file__).resolve().parents[2]

CODE_TREES = ("core", "web", "bot", "scripts", "deploy", "migrations")

# What counts as reaching for the driver: the driver, and DuckDB's own
# compaction script that the pinned image carries (`/app/scripts`).
DRIVER = ("duckdb", "compact_duckdb")

# The host tools that run on the pinned image and nowhere else, frozen. A
# file joins this list only if it is run inside `BACKUP_RESTORE_IMAGE` /
# `BACKUP_ARK_IMAGE` (deploy/archive_image.sh asks that image for DuckDB
# 1.5.5 before anything is mounted into it).
PINNED_IMAGE_TOOLS: Dict[str, str] = {
    "deploy/ark_freeze.py":
        "the second Ark's freeze, run by deploy/ark_ship.sh in the pinned image",
    "deploy/duckdb_table_fates_check.py":
        "reads the newest nightly backup's tables, run in the pinned image",
    "deploy/restore_from_export.py":
        "the restore from the Parquet archive: compact_duckdb's phases 2 and 3, "
        "from the pinned image's /app/scripts (deploy/restore-test.sh)",
}

_IMPORTERS = ("import_module", "__import__")
_MODULES_METHODS = ("get", "pop", "setdefault")


def _is_driver(name: Optional[str]) -> bool:
    return bool(name) and name.split(".")[0] in DRIVER


def _code_files() -> List[Path]:
    return sorted(p for top in CODE_TREES for p in (REPO / top).rglob("*.py")
                  if "__pycache__" not in p.parts)


def _string_constants(tree: ast.AST) -> Dict[str, str]:
    """Module-level `NAME = "…"`, so `import_module(NAME)` reads its value."""
    out: Dict[str, str] = {}
    for node in getattr(tree, "body", []):
        if (isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    out[target.id] = node.value.value
    return out


def _importer_names(tree: ast.AST) -> set:
    """Every name `import_module` or `__import__` is bound to in the file."""
    names = set(_IMPORTERS)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module in ("importlib", "builtins"):
            for alias in node.names:
                if alias.name in _IMPORTERS:
                    names.add(alias.asname or alias.name)
    return names


def _literal(node: ast.AST, constants: Dict[str, str]) -> Optional[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return constants.get(node.id)
    return None


def driver_reaches(source: str, depth: int = 0) -> Iterator[str]:
    """Every reach for the driver in `source`, as `line: form`."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return
    constants = _string_constants(tree)
    importers = _importer_names(tree)
    for node in ast.walk(tree):
        line = getattr(node, "lineno", 0)
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _is_driver(alias.name):
                    yield f"{line}: import {alias.name}"
        elif isinstance(node, ast.ImportFrom):
            if not node.level and _is_driver(node.module):
                yield f"{line}: from {node.module} import …"
        elif isinstance(node, ast.Call):
            func = node.func
            name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", None)
            if name in importers and node.args and _is_driver(_literal(node.args[0], constants)):
                yield f"{line}: {name}({_literal(node.args[0], constants)!r})"
            elif (isinstance(func, ast.Attribute) and func.attr in _MODULES_METHODS
                  and ast.unparse(func.value).endswith("modules") and node.args
                  and _is_driver(_literal(node.args[0], constants))):
                yield f"{line}: sys.modules.{func.attr}(…)"
        elif isinstance(node, ast.Subscript):
            if (ast.unparse(node.value).endswith("modules")
                    and _is_driver(_literal(node.slice, constants))):
                yield f"{line}: sys.modules[…]"
        elif (isinstance(node, ast.Constant) and isinstance(node.value, str)
              and depth < 2 and any(d in node.value for d in DRIVER)):
            # A program written out in a string: `python -c "import duckdb"`.
            for found in driver_reaches(node.value, depth + 1):
                yield f"{line}: in a string program, {found}"


def findings() -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    for path in _code_files():
        found = list(driver_reaches(path.read_text(encoding="utf-8")))
        if found:
            out[path.relative_to(REPO).as_posix()] = found
    return out


class TestNothingTheImagesRunImportsTheDriver:
    def test_no_code_file_reaches_for_it_but_the_pinned_image_s_tools(self):
        """Mutation: an `import duckdb` inside a function anywhere under
        `core/`, `web/`, `bot/` or `scripts/` — or any form the module
        docstring names — fails here naming the file and the line."""
        found = findings()
        stray = {path: lines for path, lines in found.items()
                 if path not in PINNED_IMAGE_TOOLS}
        assert not stray, f"the images carry no duckdb, and these reach for it: {stray}"

    def test_every_excused_tool_still_needs_the_excuse(self):
        """A tool that stopped importing the driver leaves the list, so the
        list stays the set of files that need the pinned image."""
        found = findings()
        stale = sorted(set(PINNED_IMAGE_TOOLS) - set(found))
        assert not stale, f"excused but importing no driver: {stale}"
        assert all(path.startswith("deploy/") for path in PINNED_IMAGE_TOOLS)

    def test_the_trees_are_walked(self):
        """Non-vacuity: the walk reads the code it exists to read."""
        rels = {p.relative_to(REPO).as_posix() for p in _code_files()}
        assert {"web/main.py", "bot/main.py", "core/store.py", "core/scheduler.py",
                "scripts/backfill_gender.py", "deploy/ark_freeze.py"} <= rels
        assert not any(r.startswith("tests/") for r in rels)

    @pytest.mark.parametrize("source", [
        "import duckdb",
        "import duckdb as d",
        "import os, duckdb.typing",
        "def f():\n    from duckdb import connect\n",
        "def f():\n    from duckdb.typing import VARCHAR\n",
        "import importlib\nimportlib.import_module('duckdb')",
        "from importlib import import_module as im\nim('duckdb')",
        "NAME = 'duckdb'\nimport importlib\nimportlib.import_module(NAME)",
        "__import__('duckdb')",
        "import sys\nsys.modules['duckdb']",
        "import sys\nsys.modules.get('duckdb')",
        "import subprocess\nsubprocess.run(['python', '-c', 'import duckdb'])",
        "def f():\n    import compact_duckdb as compact\n",
    ])
    def test_the_walk_sees_every_form(self, source):
        assert list(driver_reaches(source)), source

    @pytest.mark.parametrize("source", [
        "import core.duckdb_constants",
        "from core import duckdb_table_fates",
        "x = 'duckdb'",
        "text = 'DuckDB went in step 14'",
        "import importlib\nimportlib.import_module('core.duckdb_constants')",
        "from . import duckdb",
    ])
    def test_the_walk_does_not_read_a_name_as_the_driver(self, source):
        """Modules of this repository named after DuckDB are not the driver,
        nor is a string that merely says the word."""
        assert not list(driver_reaches(source)), source


# ─── The import, with the driver absent ─────────────────────────────────────

_PROBE = r"""
import importlib, importlib.abc, json, sys

class _NoDuckDB(importlib.abc.MetaPathFinder):
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] == "duckdb":
            raise ModuleNotFoundError("No module named 'duckdb'", name=name)
        return None

sys.meta_path.insert(0, _NoDuckDB())
sys.path.insert(0, sys.argv[2])
failed = {}
for name in json.loads(sys.argv[1]):
    try:
        importlib.import_module(name)
    except BaseException as exc:
        failed[name] = f"{type(exc).__name__}: {exc}"[:300]
print(json.dumps({"failed": failed,
                  "loaded": sorted(m for m in sys.modules if m.split(".")[0] == "duckdb")}))
"""


def _module_names() -> List[str]:
    names = []
    for top in ("core", "web", "bot"):
        for path in sorted((REPO / top).rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            rel = path.relative_to(REPO).with_suffix("")
            parts = rel.parts[:-1] if rel.name == "__init__" else rel.parts
            names.append(".".join(parts))
    names += [f"scripts.{p.stem}" for p in sorted((REPO / "scripts").glob("*.py"))]
    return names


class TestEverythingImportsWithoutTheDriver:
    def test_every_module_imports_with_duckdb_absent(self, tmp_path):
        """A fresh interpreter, run from a scratch directory with test
        secrets, so no `.env` of the checkout is read and nothing an earlier
        test loaded counts. Mutation: `import duckdb` at the top of any module
        under `core/`, `web/` or `bot/` fails here naming it."""
        modules = _module_names()
        assert {"web.main", "bot.main", "core.store", "core.scheduler",
                "scripts.backfill_gender"} <= set(modules)
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(tmp_path),
               "TZ": "UTC", "PYTHONDONTWRITEBYTECODE": "1",
               "DASHBOARD_SECRET_KEY": "s14-19", "BOT_TOKEN": "1:s14-19",
               "KEYCRM_API_KEY": "s14-19", "KS_ALERTS_DISABLED": "1"}
        run = subprocess.run(
            [sys.executable, "-I", "-c", _PROBE, json.dumps(modules), str(REPO)],
            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=300)
        assert run.returncode == 0, run.stderr[-3000:]
        result = json.loads(run.stdout.strip().splitlines()[-1])
        assert result["failed"] == {}, result["failed"]
        assert result["loaded"] == []

    def test_the_probe_would_see_the_driver(self, tmp_path):
        """Non-vacuity: with the driver absent, a module that imports it
        fails the probe."""
        (tmp_path / "needs_driver.py").write_text("import duckdb\n")
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": str(tmp_path)}
        run = subprocess.run(
            [sys.executable, "-I", "-c", _PROBE, json.dumps(["needs_driver"]), str(tmp_path)],
            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=60)
        result = json.loads(run.stdout.strip().splitlines()[-1])
        assert "ModuleNotFoundError" in result["failed"]["needs_driver"]
