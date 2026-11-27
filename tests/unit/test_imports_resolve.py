"""Every name the tree imports from `core`, `web` or `bot` still exists.

Step 14 deletes DuckDB from the code one PR at a time, and most of what it
deletes is imported somewhere: lazily inside a function, under another name,
as `module.attr` after `from core import module`, or by a string handed to
`importlib.import_module`. A deleted name that one of those still asks for
fails only when that line runs — a job at 05:30, an except branch, a route
nobody calls in a test — and the plan's own review found three such lines
the first draft would have broken (`.planning/DUCKDB_EXIT_STEP14_PLAN.md`,
rule 14). A test walks the whole tree instead of trusting a list of
importers, because a guard that names its subjects only guards the ones
somebody was already thinking about.

It checks, in every Python file of the repository (the suite included, and
at any depth of a function):

* `from core.X import Y` and `from core import X` — the module imports and
  `Y` is an attribute or submodule of it;
* `import core.X` / `import core.X as Y` — the module imports;
* `X.attr` (read, not assigned), where `X` is bound in that scope or an
  enclosing one to a module by one of those imports — `attr` exists on the
  module; a dotted chain is followed while it stays a module;
* `importlib.import_module("core.…")` with a literal — the module imports.

The same for `web` and `bot`. An import written to fail — inside
`pytest.raises(...)` or a `try` whose handler names `ImportError` or
`ModuleNotFoundError` — is not judged. One inside `except Exception`,
`except BaseException` or a bare `except:` is: that is how this codebase
writes "never raises", and it is where a lazy import of a deleted name hides.
"""
from __future__ import annotations

import ast
import importlib
import os
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

PACKAGES = ("core", "web", "bot")

SKIP_DIRS = {"node_modules", "data", "__pycache__", "site-packages", "static-v2",
             "storybook-static", "frontend"}


def _sources():
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = sorted(
            d for d in dirnames
            if d not in SKIP_DIRS and not d.startswith(".")
            and not os.path.exists(os.path.join(dirpath, d, "pyvenv.cfg")))
        for name in sorted(filenames):
            if name.endswith(".py"):
                path = Path(dirpath) / name
                yield path.relative_to(ROOT).as_posix(), path


def _ours(module: str, packages=PACKAGES) -> bool:
    return module.split(".", 1)[0] in packages


# Only a handler that names the import failure itself says the import was
# written to fail. `except Exception`, `except BaseException` and a bare
# `except:` are this codebase's "never raises" shape, and a lazy import of a
# deleted name inside one fails exactly where nobody looks: the off verdict's
# call in the scheduler sat in one, and so did `/api/health`'s
# `from core.config import DEFAULT_TZ`, which published `sync: null` for
# eight months. Those are judged like any other import.
_IMPORT_ERRORS = {"ImportError", "ModuleNotFoundError"}


def _handler_names(kinds: "ast.expr | None") -> list[str]:
    if kinds is None:
        return []
    elts = kinds.elts if isinstance(kinds, ast.Tuple) else [kinds]
    return [n.id if isinstance(n, ast.Name) else n.attr if isinstance(n, ast.Attribute)
            else "" for n in elts]


def _expects_failure(node: ast.AST, parents) -> bool:
    """Inside `with pytest.raises(...)`, or a `try` whose handler names
    `ImportError` or `ModuleNotFoundError`: an import written to fail."""
    child = node
    for parent in parents:
        if isinstance(parent, (ast.With, ast.AsyncWith)):
            for item in parent.items:
                call = item.context_expr
                if (isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
                        and call.func.attr == "raises"):
                    return True
        if isinstance(parent, ast.Try) and child in parent.body:
            for handler in parent.handlers:
                if any(n in _IMPORT_ERRORS for n in _handler_names(handler.type)):
                    return True
        child = parent
    return False


_SCOPES = (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)


class _Walk(ast.NodeVisitor):
    """Collects, per scope, the names bound to one of our modules by an import
    and the names bound by anything else; then judges every read attribute of
    a name that resolves to a module."""

    def __init__(self, rel: str, packages=PACKAGES):
        self.rel = rel
        self.packages = packages
        self.problems: list[str] = []
        self.parents: list[ast.AST] = []
        # scope node -> {name: module dotted name}
        self.imports: dict[ast.AST, dict[str, str]] = {}
        # scope node -> names bound otherwise
        self.others: dict[ast.AST, set[str]] = {}
        self.scope_parent: dict[ast.AST, ast.AST] = {}
        self.reads: list[tuple[ast.AST, ast.Attribute]] = []
        self.scope_stack: list[ast.AST] = []

    # ─── scopes ──────────────────────────────────────────────────────────
    def _enter(self, node):
        if self.scope_stack:
            self.scope_parent[node] = self.scope_stack[-1]
        self.scope_stack.append(node)
        self.imports.setdefault(node, {})
        self.others.setdefault(node, set())

    def generic_visit(self, node):
        self.parents.insert(0, node)
        super().generic_visit(node)
        self.parents.pop(0)

    def visit_Module(self, node):
        self._enter(node)
        self.generic_visit(node)
        self.scope_stack.pop()

    def _visit_function(self, node):
        if not isinstance(node, ast.Lambda):
            self.others[self.scope_stack[-1]].add(node.name)
            for deco in node.decorator_list:
                self.visit(deco)
        self._enter(node)
        args = node.args
        for arg in (*args.posonlyargs, *args.args, *args.kwonlyargs,
                    *(a for a in (args.vararg, args.kwarg) if a)):
            self.others[node].add(arg.arg)
        self.parents.insert(0, node)
        for child in (node.body if isinstance(node.body, list) else [node.body]):
            self.visit(child)
        self.parents.pop(0)
        self.scope_stack.pop()

    visit_FunctionDef = visit_AsyncFunctionDef = visit_Lambda = _visit_function

    def visit_ClassDef(self, node):
        self.others[self.scope_stack[-1]].add(node.name)
        self.generic_visit(node)

    # ─── bindings ────────────────────────────────────────────────────────
    def visit_Name(self, node):
        if isinstance(node.ctx, (ast.Store, ast.Del)):
            self.others[self.scope_stack[-1]].add(node.id)

    def visit_ExceptHandler(self, node):
        if node.name:
            self.others[self.scope_stack[-1]].add(node.name)
        self.generic_visit(node)

    def _bind_unjudged(self, node):
        """An import written to fail binds its names all the same: a read
        through one is not judged, and not misread as a read through an
        enclosing scope's import of the same name."""
        scope = self.scope_stack[-1]
        for alias in node.names:
            if alias.name != "*":
                self.others[scope].add((alias.asname or alias.name).split(".")[0])

    def visit_Import(self, node):
        if _expects_failure(node, self.parents):
            self._bind_unjudged(node)
            return
        scope = self.scope_stack[-1]
        for alias in node.names:
            if not _ours(alias.name, self.packages):
                self.others[scope].add((alias.asname or alias.name).split(".")[0])
                continue
            self._check_module(alias.name, node)
            if alias.asname:
                self.imports[scope][alias.asname] = alias.name
            else:
                top = alias.name.split(".")[0]
                self.imports[scope][top] = top

    def visit_ImportFrom(self, node):
        if node.level or not node.module or not _ours(node.module, self.packages):
            scope = self.scope_stack[-1]
            for alias in node.names:
                self.others[scope].add(alias.asname or alias.name)
            return
        if _expects_failure(node, self.parents):
            self._bind_unjudged(node)
            return
        module = self._check_module(node.module, node)
        scope = self.scope_stack[-1]
        for alias in node.names:
            if alias.name == "*":
                continue
            bound = alias.asname or alias.name
            full = f"{node.module}.{alias.name}"
            if module is None:
                continue
            if hasattr(module, alias.name):
                if isinstance(getattr(module, alias.name), types.ModuleType):
                    self.imports[scope][bound] = getattr(module, alias.name).__name__
                else:
                    self.others[scope].add(bound)
                continue
            try:
                sub = importlib.import_module(full)
            except ModuleNotFoundError as exc:
                if exc.name and full.startswith(exc.name) or exc.name == full:
                    self.problems.append(
                        f"{self.rel}:{node.lineno}: from {node.module} import "
                        f"{alias.name} — no such name")
                    continue
                raise
            self.imports[scope][bound] = sub.__name__

    def visit_Call(self, node):
        func = node.func
        name = (func.attr if isinstance(func, ast.Attribute)
                else func.id if isinstance(func, ast.Name) else None)
        if (name == "import_module" and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
                and _ours(node.args[0].value, self.packages)
                and not _expects_failure(node, self.parents)):
            self._check_module(node.args[0].value, node)
        self.generic_visit(node)

    def visit_Attribute(self, node):
        if isinstance(node.ctx, ast.Load):
            root = node
            while isinstance(root, ast.Attribute):
                root = root.value
            if isinstance(root, ast.Name):
                self.reads.append((self.scope_stack[-1], node))
                # The innermost chain is recorded; its parts are not visited
                # again, so `a.b.c` is judged once, from `a`.
                return
        self.generic_visit(node)

    # ─── judging ─────────────────────────────────────────────────────────
    def _check_module(self, dotted: str, node) -> "types.ModuleType | None":
        try:
            return importlib.import_module(dotted)
        except ModuleNotFoundError as exc:
            if exc.name and (dotted == exc.name or dotted.startswith(exc.name + ".")):
                self.problems.append(f"{self.rel}:{node.lineno}: {dotted} — no such module")
                return None
            raise

    def _resolve(self, scope, name):
        while scope is not None:
            if name in self.imports.get(scope, {}):
                return self.imports[scope][name]
            if name in self.others.get(scope, set()):
                return None
            scope = self.scope_parent.get(scope)
        return None

    def judge(self):
        for scope, node in self.reads:
            chain = []
            cursor = node
            while isinstance(cursor, ast.Attribute):
                chain.append(cursor.attr)
                cursor = cursor.value
            chain.reverse()
            dotted = self._resolve(scope, cursor.id)
            if dotted is None:
                continue
            try:
                obj = importlib.import_module(dotted)
            except ModuleNotFoundError:
                continue      # reported where it was imported
            path = dotted
            for attr in chain:
                if not isinstance(obj, types.ModuleType):
                    break
                if hasattr(obj, attr):
                    obj = getattr(obj, attr)
                    path = f"{path}.{attr}"
                    continue
                try:
                    obj = importlib.import_module(f"{obj.__name__}.{attr}")
                    path = f"{path}.{attr}"
                except ModuleNotFoundError:
                    self.problems.append(
                        f"{self.rel}:{node.lineno}: {cursor.id}.{'.'.join(chain)} — "
                        f"{path} has no {attr!r}")
                    break


def _problems(rel: str, path: Path, packages=PACKAGES) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=rel)
    walk = _Walk(rel, packages)
    walk.visit(tree)
    walk.judge()
    return walk.problems


def test_every_import_of_ours_resolves():
    problems: list[str] = []
    for rel, path in _sources():
        problems += _problems(rel, path)
    assert not problems, "\n".join(problems)


# ─── The guard sees what it is for ───────────────────────────────────────────
#
# Shown a package of the test's own, so that what the walker is judged on never
# depends on which modules the application holds today.

PROBE = "ks_imports_probe"


@pytest.fixture
def probe(tmp_path, monkeypatch):
    """`ks_imports_probe`, holding one module with one name, importable for
    the test and forgotten after it."""
    package = tmp_path / PROBE
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "present.py").write_text("name = 1\n")
    monkeypatch.syspath_prepend(str(tmp_path))
    yield tmp_path
    for module in [m for m in sys.modules if m == PROBE or m.startswith(PROBE + ".")]:
        del sys.modules[module]


def _judged(root: Path, source: str) -> list[str]:
    path = root / "probe_source.py"
    path.write_text(source)
    return _problems("probe_source.py", path, packages=(PROBE,))


@pytest.mark.parametrize("source, line, missing", [
    (f"import {PROBE}.gone\n", 1, f"{PROBE}.gone"),
    # The "never raises" shape: a lazy import inside `except Exception`, as
    # `/api/health`'s import of a name `core.config` never had, which
    # published `sync: null` from 2026-02-12 to 2026-10-21.
    (f"def f():\n    try:\n        from {PROBE}.present import gone\n"
     "    except Exception:\n        return None\n", 3, "gone"),
    (f"from {PROBE} import present as p\np.gone()\n", 2, "gone"),
    (f"import importlib\nimportlib.import_module('{PROBE}.gone')\n", 2, f"{PROBE}.gone"),
], ids=["module", "lazy_name_under_except_exception", "attribute_through_alias",
        "module_by_string"])
def test_it_finds(probe, source, line, missing):
    """Mutation: each shape step 14 could leave behind — a deleted module, a
    deleted name imported lazily where an exception is swallowed, a deleted
    attribute read under another name, a module asked for by string — is
    reported, at its line."""
    problems = _judged(probe, source)
    assert len(problems) == 1, problems
    assert problems[0].startswith(f"probe_source.py:{line}:") and missing in problems[0], problems


def test_it_leaves_alone(probe):
    """What is not a read of a missing name: a name that exists, a parameter
    shadowing the module, an assignment, an import written to fail."""
    assert _judged(probe, (
        f"from {PROBE} import present\n"
        "present.name\n"
        "def f(present):\n    return present.anything\n"
        "present.assigned = 1\n"
        f"try:\n    from {PROBE} import gone\nexcept ImportError:\n    gone = None\n"
    )) == []
