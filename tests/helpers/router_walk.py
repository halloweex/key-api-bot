"""What reaches round a read router to an engine module, read off the AST.
Shared by the routers' Postgres-only guards.
"""
from __future__ import annotations

import ast
from typing import List, Set


OLD_SHAPE = {"fall_back", "no_address", "no_engine", "enabled", "available"}


# Postgres (`core.pg`, `core.pg_*`), the driver, and — since step 14's S14-07,
# whose cohorts read ClickHouse alone — ClickHouse's modules (`core.ch_*`).
ENGINE_MODULES = ("core.pg", "core.ch_", "asyncpg")


def _dotted(node) -> str:
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if isinstance(node, ast.Name):
        parts.append(node.id)
    return ".".join(reversed(parts))


def _what_reaches_round_the_router(source: str):
    """`(routers, reaches)` for one module: the `_*_run` functions — rule 5
    of `test_read_fallback_sites.py` judges those — and, for every other
    function, what it reaches that only a router may: an engine (a function
    of a `core.pg*` module — or, since step 14's S14-07, of a `core.ch_*`
    one — by the module or imported by name; `get_pool`; an import of
    `core.pg*`, `core.ch_*` or `asyncpg` of its own), the old shape's
    machinery (`fall_back`, `no_address`, `no_engine`, `enabled`,
    `available`), or the environment (`os.getenv`, `os.environ`, a
    `KS_READ_*` name outside a docstring)."""
    tree = ast.parse(source)
    modules, functions = set(), set()
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module:
            if n.module == "core":
                modules |= {a.asname or a.name for a in n.names
                            if a.name.startswith(("pg", "ch_"))}
            elif n.module.startswith(ENGINE_MODULES):
                functions |= {a.asname or a.name for a in n.names}
        elif isinstance(n, ast.Import):
            modules |= {a.asname for a in n.names
                        if a.asname and a.name.startswith(ENGINE_MODULES)}
    routers, reaches = set(), {}
    for fn in ast.walk(tree):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if fn.name.startswith("_") and fn.name.endswith("_run"):
            routers.add(fn.name)
            continue
        docstrings = {id(f.body[0].value) for f in ast.walk(fn)
                      if isinstance(f, (ast.FunctionDef, ast.AsyncFunctionDef))
                      and f.body and isinstance(f.body[0], ast.Expr)}
        found = []
        for n in ast.walk(fn):
            if isinstance(n, (ast.Import, ast.ImportFrom)):
                names = ([n.module or ""] if isinstance(n, ast.ImportFrom)
                         else [a.name for a in n.names])
                if isinstance(n, ast.ImportFrom) and n.module == "core":
                    names = [f"core.{a.name}" for a in n.names]
                found += [f"engine: imports {m}" for m in names
                          if m.startswith(ENGINE_MODULES)]
            elif isinstance(n, ast.Call):
                name = _dotted(n.func)
                head, last = name.split(".", 1)[0], name.rsplit(".", 1)[-1]
                if (head in modules or name in functions
                        or name.startswith(ENGINE_MODULES) or last == "get_pool"):
                    found.append(f"engine: {name}()")
                elif last in OLD_SHAPE:
                    found.append(f"old shape: {name}()")
                elif name in ("os.getenv", "getenv", "os.environ.get", "environ.get"):
                    found.append(f"environment: {name}()")
            elif isinstance(n, ast.Subscript) and _dotted(n.value) in ("os.environ", "environ"):
                found.append("environment: os.environ[...]")
            elif (isinstance(n, ast.Constant) and isinstance(n.value, str)
                  and "KS_READ_" in n.value and id(n) not in docstrings):
                found.append(f"environment: {n.value[:40]!r}")
        if found:
            reaches[fn.name] = found
    return routers, reaches


def _judged_by_rule_5(fn: ast.AST, aliases: Set[str], names: Set[str]) -> List[str]:
    """The engine reaches of `fn`, in the words of S14-05's walk, that rule 5
    of `test_read_fallback_sites.py` judges: every call its `_engine_calls`
    returns, and every import in `fn` binding what those calls are read by —
    a module by its alias (`from core import pg_x`, `import core.pg_x as y`),
    spelled out (`import core.pg_x`), or a function by its name (`from
    core.pg_x import y`) — and the same of a `core.ch_*` module, which rule 5
    reads since step 14's S14-07. `fn`'s own nodes only, as rule 5 reads
    it."""
    from tests.helpers.engine_walk import (
        ENGINE_PREFIXES, _engine_calls, _is_engine_module, _own_nodes,
    )
    from tests.helpers.router_walk import _dotted

    judged = [f"engine: {_dotted(c.func)}()" for c in _engine_calls(fn, aliases, names)]
    for n in _own_nodes(fn):
        if isinstance(n, ast.ImportFrom) and n.module == "core":
            judged += [f"engine: imports core.{a.name}" for a in n.names
                       if a.name.startswith(ENGINE_PREFIXES)]
        elif isinstance(n, ast.ImportFrom) and n.module and _is_engine_module(n.module):
            judged.append(f"engine: imports {n.module}")
        elif isinstance(n, ast.Import):
            judged += [f"engine: imports {a.name}" for a in n.names
                       if _is_engine_module(a.name)]
    return judged


def _engine_held_as_a_value(fn: ast.AST, aliases: Set[str], names: Set[str]) -> List[str]:
    """Where `fn` holds an engine module or function other than as the
    callee of a call rule 5 judges. `fetch = pg_gold_read.fetch_series`, then
    `await fetch(...)` outside the `try`, is a call neither walk sees."""
    from tests.helpers.engine_walk import (
        _engine_calls, _names_an_engine_module, _own_nodes,
    )
    from tests.helpers.router_walk import _dotted

    callees = set()
    for call in _engine_calls(fn, aliases, names):
        node = call.func
        while isinstance(node, ast.Attribute):
            callees.add(id(node))
            node = node.value
        callees.add(id(node))
    held = []
    for n in _own_nodes(fn):
        if id(n) in callees:
            continue
        if (isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)
                and (n.id in aliases or n.id in names or n.id == "get_pool")):
            held.append(f"engine: {n.id} held as a value")
        elif isinstance(n, ast.Attribute) and _names_an_engine_module(n):
            held.append(f"engine: {_dotted(n)} held as a value")
    return held


def round_a_refusal(source: str, relpath: str) -> List[str]:
    """What in `source` reaches what only a router or a refusing helper may,
    and is neither. S14-05's walk of the tab modules
    (`test_read_routers_postgres_only._what_reaches_round_the_router`: an
    engine, the old shape's machinery, the environment), with one allowance:
    a function rule 5 of `test_read_fallback_sites.py` holds clean — it
    refuses, and every engine call it makes sits inside the `try` whose
    handler is the refusal — may reach an engine, but only by what rule 5
    judges. Every other engine reach of it is still reported: S14-05's walk
    sees calls rule 5 does not (a function imported by its name was one,
    until the review of this PR's merge widened rule 5), and an engine held
    as a value is called by a name no walk reads. A name defined twice in
    the file gets no allowance, since the walk keys its reaches by name."""
    from collections import Counter

    from tests.helpers.engine_walk import (
        _engine_aliases, _engine_names, _functions, read_switch_modules, walk_source,
    )
    from tests.helpers.router_walk import _what_reaches_round_the_router

    tree = ast.parse(source)
    aliases, names = _engine_aliases(tree), _engine_names(tree)
    _routers, reaches = _what_reaches_round_the_router(source)
    defined = Counter(getattr(fn, "name", "") for fn in _functions(tree))
    clean = {s.function for s in walk_source(source, relpath, read_switch_modules())
             if s.rule == "refuse" and s.ok and defined[s.function] == 1}
    out = []
    for function, found in sorted(reaches.items()):
        left = Counter(found)
        if function in clean:
            fn = next(f for f in _functions(tree) if getattr(f, "name", "") == function)
            left -= Counter(_judged_by_rule_5(fn, aliases, names))
            left += Counter(_engine_held_as_a_value(fn, aliases, names))
        out += [f"{function}: {f}" for f in sorted(left.elements())]
    return out
