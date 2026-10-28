"""Does a function reach the DuckDB file? Read off its syntax, never its text.

Step 14's S14-08 left `/sms` and the access lists with one store, Postgres.
What keeps it that way is that nothing on those paths reaches for the file —
and a guard that looks for `self.connection` passes every other way to it: a
helper over the connection (`self._fetch_all(...)`), `super()`, `getattr(store,
"connection")`, a store fetched afresh, the opener itself. The review of
S14-08 put a DuckDB tail back on `_sms_run`, `_users_run` and the audience in
three of those spellings, and each passed every guard but the entry-points
walk — which itself sees neither `super()` nor `getattr`. So the ways are named
once, here, for the files that hold those paths to them:
`tests/unit/test_duckdb_table_fates.py`, `tests/unit/test_pg_sms_read.py` and
`tests/unit/test_dashboard_users_store.py`.

What it cannot see: a name computed at run time (`getattr(store, name)` with
`name` a variable), and a way to the file that goes through a function the
walk does not open — which is what the entry-points walk
(`tests/unit/test_duckdb_off_entry_points.py`) is for.
"""
from __future__ import annotations

import ast
import inspect
import textwrap
from typing import Any, FrozenSet, List, Optional, Sequence, Set

# `DuckDBStore`'s ways to the file: `connection()` and every helper over it,
# its own open and checkpoint, and the file's one opener.
FILE_REACHES: FrozenSet[str] = frozenset({
    "connection", "_fetch_one", "_fetch_all", "_offload",
    "connect", "checkpoint", "open_file",
})
# Modules whose import is a way to the file by itself.
FILE_MODULES: Sequence[str] = ("duckdb", "core.duckdb_switch")

# A router hands its statement to Postgres and to nothing else, so it may not
# touch the store at all — not even to fetch it.
ROUTER_REACHES: FrozenSet[str] = FILE_REACHES | {"get_store"}
ROUTER_MODULES: Sequence[str] = tuple(FILE_MODULES) + ("core.duckdb_store",)

_IMPORTERS = ("import_module", "__import__")


def function(obj: Any) -> "ast.FunctionDef | ast.AsyncFunctionDef":
    """The definition of a function or method, decorators unwrapped."""
    source = textwrap.dedent(inspect.getsource(inspect.unwrap(obj)))
    node = ast.parse(source).body[0]
    assert isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)), obj
    return node


def functions(tree: ast.AST) -> List["ast.FunctionDef | ast.AsyncFunctionDef"]:
    return [n for n in ast.walk(tree)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]


def names(tree: ast.AST) -> Set[str]:
    """Every name the code spells: bare, as an attribute, imported (under
    either name), or as a string that is an identifier (`getattr(m, "X")`)."""
    found: Set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name):
            found.add(node.id)
        elif isinstance(node, ast.Attribute):
            found.add(node.attr)
        elif isinstance(node, ast.alias):
            found.add(node.name.rsplit(".", 1)[-1])
            if node.asname:
                found.add(node.asname)
        elif (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and node.value.isidentifier()):
            found.add(node.value)
    return found


def _is_module(name: str, modules: Sequence[str]) -> bool:
    return any(name == m or name.startswith(m + ".") for m in modules)


def reaches(fn: ast.AST, *, self_attrs: Optional[FrozenSet[str]] = None,
            reach: FrozenSet[str] = FILE_REACHES,
            modules: Sequence[str] = FILE_MODULES) -> List[str]:
    """Where `fn` reaches the file, as `what at line n`; empty when nowhere.

    A way is a name in `reach` — bare (`open_file(...)`), as an attribute of
    anything (`self.connection()`, `super()._fetch_all(...)`,
    `store.connection()`), or as a string (`getattr(store, "connection")`) —
    or an import of one of `modules` or of a name in `reach`, `import_module`
    included.

    With `self_attrs` given, `fn` is held to more: `self` may appear only as
    `self.<attr>` for an attr in it (none, for an empty set), and `super` not at
    all. That is a router's rule — every way from the store to the file goes
    through `self`, so a router that names it for anything but rendering its
    statement has a way back to DuckDB that no name list can enumerate
    (`store = self`, then anything).
    """
    parents = {id(child): parent for parent in ast.walk(fn)
               for child in ast.iter_child_nodes(parent)}
    found: List[str] = []
    for node in ast.walk(fn):
        line = getattr(node, "lineno", "?")
        if isinstance(node, ast.Attribute) and node.attr in reach:
            found.append(f".{node.attr} at line {line}")
        elif isinstance(node, ast.Name) and node.id in reach:
            found.append(f"{node.id} at line {line}")
        elif (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and node.value in reach):
            found.append(f"{node.value!r} at line {line}")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if _is_module(alias.name, modules):
                    found.append(f"import {alias.name} at line {line}")
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            for alias in node.names:
                if (_is_module(module, modules)
                        or _is_module(f"{module}.{alias.name}", modules)
                        or alias.name in reach):
                    found.append(f"from {module} import {alias.name} at line {line}")
        elif isinstance(node, ast.Call):
            callee = node.func
            called = (callee.attr if isinstance(callee, ast.Attribute)
                      else callee.id if isinstance(callee, ast.Name) else None)
            if called in _IMPORTERS and node.args:
                first = node.args[0]
                if (isinstance(first, ast.Constant) and isinstance(first.value, str)
                        and _is_module(first.value, modules)):
                    found.append(f"{called}({first.value!r}) at line {line}")
        if self_attrs is not None and isinstance(node, ast.Name):
            if node.id == "super":
                found.append(f"super at line {line}")
            elif node.id == "self":
                parent = parents.get(id(node))
                if not (isinstance(parent, ast.Attribute) and parent.value is node
                        and parent.attr in self_attrs):
                    used = (f"self.{parent.attr}"
                            if isinstance(parent, ast.Attribute) and parent.value is node
                            else "self")
                    found.append(f"{used} at line {line}")
    return found
