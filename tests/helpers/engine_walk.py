"""The AST walk over the read routers: which call reaches an engine, and where
its failure is refused. Shared by the read-fallback guards.
"""
from __future__ import annotations

import ast
import pathlib
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set

import pytest


REPO = pathlib.Path(__file__).resolve().parents[2]


WALKED = ("core", "web")


FALLBACK_TEXT = re.compile(r"falling back to DuckDB", re.IGNORECASE)


@dataclass(frozen=True)
class Site:
    rule: str          # "log" | "gate" | "handler" | "refuse"
    path: str
    line: int
    function: str
    ok: bool


def _own_nodes(node: ast.AST) -> Iterable[ast.AST]:
    """Every node under `node`, not descending into nested functions,
    lambdas or classes — those are judged as functions of their own."""
    stack = list(ast.iter_child_nodes(node))
    while stack:
        child = stack.pop()
        yield child
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef,
                              ast.Lambda, ast.ClassDef)):
            continue
        stack.extend(ast.iter_child_nodes(child))


def _call_name(call: ast.Call) -> Optional[str]:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


def _functions(tree: ast.AST) -> List[ast.AST]:
    return [n for n in ast.walk(tree)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]


def _mentions_read_unavailable(handler: ast.ExceptHandler) -> bool:
    if handler.type is None:
        return False
    for n in ast.walk(handler.type):
        if isinstance(n, ast.Name) and n.id == "ReadUnavailable":
            return True
        if isinstance(n, ast.Attribute) and n.attr == "ReadUnavailable":
            return True
    return False


def _decides(handler: ast.ExceptHandler, try_node: ast.Try) -> bool:
    """The handler re-raises, or the `try` it belongs to has already said
    what `ReadUnavailable` means. (Counting a fallback through
    `read_fallback.fall_back` was the other answer until step 14's S14-16
    deleted it.)"""
    for n in _own_nodes(handler):
        if isinstance(n, ast.Raise):
            return True
    for earlier in try_node.handlers:
        if _mentions_read_unavailable(earlier):
            return True
        if earlier is handler:
            break
    return False


def _handlers(fn: ast.AST) -> List[tuple]:
    """(handler, its try) for every handler owned by `fn`."""
    out = []
    for n in _own_nodes(fn):
        if isinstance(n, (ast.Try, getattr(ast, "TryStar", ast.Try))):
            for h in n.handlers:
                out.append((h, n))
    return out


def _is_gate(node: ast.AST) -> bool:
    if not (isinstance(node, ast.BoolOp) and isinstance(node.op, ast.And)):
        return False
    called = {_call_name(n) for v in node.values for n in ast.walk(v)
              if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    return {"enabled", "available"} <= called


def _string_parts(node: ast.AST) -> List[str]:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.JoinedStr):
        return [v.value for v in node.values
                if isinstance(v, ast.Constant) and isinstance(v.value, str)]
    return []


def read_switch_modules() -> Set[str]:
    """Modules in `core/` whose `ENV` names a `KS_READ_*` variable and which
    answer `enabled()` — a switch choosing an engine. `core/read_fallback.py`
    has the prefix and no `enabled`: it chooses how a failure degrades, not
    where a read goes."""
    found = set()
    for path in sorted((REPO / "core").glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        names_env = any(
            isinstance(stmt, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "ENV" for t in stmt.targets)
            and isinstance(stmt.value, ast.Constant)
            and str(stmt.value.value).startswith("KS_READ_")
            for stmt in tree.body)
        answers = any(isinstance(stmt, ast.FunctionDef) and stmt.name == "enabled"
                      for stmt in tree.body)
        if names_env and answers:
            found.add(path.stem)
    return found


def _switch_names(tree: ast.AST, switches: Set[str]) -> tuple:
    """(module aliases, function names) this file binds to a read switch."""
    modules, functions = set(), set()
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module == "core":
            for a in n.names:
                if a.name in switches:
                    modules.add(a.asname or a.name)
        elif (isinstance(n, ast.ImportFrom) and n.module
              and n.module.startswith("core.")
              and n.module.split(".", 1)[1] in switches):
            for a in n.names:
                functions.add(a.asname or a.name)
        elif isinstance(n, ast.Import):
            for a in n.names:
                if a.name.startswith("core.") and a.name.split(".", 1)[1] in switches:
                    modules.add(a.asname or a.name)
    return modules, functions


def _reaches_another_engine(fn: ast.AST, modules: Set[str], functions: Set[str]) -> bool:
    for n in ast.walk(fn):
        if not isinstance(n, ast.Call):
            continue
        name = _call_name(n)
        if name == "get_pool":
            return True
        if name and name.endswith("_run") and name != "_run":
            return True
        if (isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name)
                and n.func.value.id in modules
                and name not in ("enabled", "available")):
            return True
        if isinstance(n.func, ast.Name) and n.func.id in functions:
            return True
    return False


def _in_handler_scope(relpath: str) -> bool:
    return (relpath.startswith("core/repositories/")
            or re.fullmatch(r"core/pg_[^/]*read[^/]*\.py", relpath) is not None)


# Rule 5. What a router with no tail may not reach: DuckDB through the store,
# and the gate's or the fallback's machinery, which belong to the old shape.
TAIL_READS = {"connection", "_fetch_one", "_fetch_all", "execute"}


# The old shape's names: the gate's, and `read_fallback`'s fallback machinery,
# deleted in step 14's S14-16 — a call of one there would not even resolve.
OLD_SHAPE = {"fall_back", "no_address", "no_engine", "enabled", "available"}


BROAD = {"Exception", "BaseException"}


# The engines a router reaches: Postgres' modules, and since step 14's
# S14-07 ClickHouse's, whose cohort reader is the cohorts' one engine.
ENGINE_PREFIXES = ("pg_", "ch_")


def _engine_aliases(tree: ast.AST) -> Set[str]:
    """Every name the file binds to a `core.pg_*` or `core.ch_*` module, at
    the top or inside a function: `from core import pg_traffic_read`,
    `import core.pg_x as y`."""
    found = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module == "core":
            for a in n.names:
                if a.name.startswith(ENGINE_PREFIXES):
                    found.add(a.asname or a.name)
        elif isinstance(n, ast.Import):
            for a in n.names:
                if (a.name.startswith(tuple(f"core.{p}" for p in ENGINE_PREFIXES))
                        and a.asname):
                    found.add(a.asname)
    return found


def _is_engine_module(dotted: str) -> bool:
    """`core.pg`, or a `core.pg_*` or `core.ch_*` module (`ENGINE_PREFIXES`)."""
    return dotted == "core.pg" or dotted.startswith(
        tuple(f"core.{p}" for p in ENGINE_PREFIXES))


def _engine_names(tree: ast.AST) -> Set[str]:
    """Every name the file imports from `core.pg` or a `core.pg_*` or
    `core.ch_*` module, at the top or inside a function — only a call of one
    counts, so a constant among them changes nothing: `from
    core.pg_gold_read import fetch_series`, `from core.pg import get_pool as
    gp`, `from core.ch_cohorts import fetch`. Until the review of step 14's
    S14-06 merge the rule judged `<module>.<fn>(...)` and a bare
    `get_pool(...)` alone, so a call by such a name outside every `try` was
    an uncounted 500 it never saw; ClickHouse's modules, the cohorts' one
    engine since S14-07, are read in every spelling since S14-07's merge of
    that review."""
    found = set()
    for n in ast.walk(tree):
        if isinstance(n, ast.ImportFrom) and n.module and _is_engine_module(n.module):
            found |= {a.asname or a.name for a in n.names}
    return found


def _names_an_engine_module(node: ast.AST) -> bool:
    """`core.pg`, `core.pg_*` or `core.ch_*` spelled out: `import
    core.pg_x` binds no alias, so its calls are `core.pg_x.fetch(...)`."""
    parts = []
    while isinstance(node, ast.Attribute):
        parts.append(node.attr)
        node = node.value
    if not (isinstance(node, ast.Name) and node.id == "core" and parts):
        return False
    return _is_engine_module(f"core.{parts[-1]}")


def _engine_calls(fn: ast.AST, aliases: Set[str], names: Set[str]) -> List[ast.Call]:
    """The engine calls in `fn`'s own nodes: a function of an engine module
    by the module's alias or spelled out, a function imported by its name
    (`names`, `_engine_names` — required, so a caller cannot forget it), and
    `get_pool` however it is reached."""
    out = []
    for n in _own_nodes(fn):
        if not isinstance(n, ast.Call):
            continue
        if _call_name(n) == "get_pool":
            out.append(n)
        elif (isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name)
              and n.func.value.id in aliases):
            out.append(n)
        elif isinstance(n.func, ast.Name) and n.func.id in names:
            out.append(n)
        elif isinstance(n.func, ast.Attribute) and _names_an_engine_module(n.func.value):
            out.append(n)
    return out


def _is_read_fallback_refuse(call: ast.Call) -> bool:
    """`read_fallback.refuse(...)` — qualified, because a route's validation
    helper is called `refuse` too (`web/routes/api/customers.py`)."""
    f = call.func
    return (isinstance(f, ast.Attribute) and f.attr == "refuse"
            and isinstance(f.value, ast.Name) and f.value.id == "read_fallback")


def _refuses_it(handler: ast.ExceptHandler) -> bool:
    """`except Exception as exc: raise read_fallback.refuse("<surface>", exc)`
    and nothing else."""
    caught = handler.type
    if caught is not None and not (
            isinstance(caught, ast.Name) and caught.id in BROAD):
        return False
    if handler.name is None or len(handler.body) != 1:
        return False
    stmt = handler.body[0]
    if not (isinstance(stmt, ast.Raise) and stmt.cause is None
            and isinstance(stmt.exc, ast.Call)):
        return False
    call = stmt.exc
    if not _is_read_fallback_refuse(call) or len(call.args) != 2 or call.keywords:
        return False
    surface, cause = call.args
    return (isinstance(surface, ast.Constant) and isinstance(surface.value, str)
            and re.fullmatch(r"[a-z_]+", surface.value) is not None
            and isinstance(cause, ast.Name) and cause.id == handler.name)


def _refused_where_it_fails(call: ast.Call, fn: ast.AST,
                            parent: Dict[ast.AST, ast.AST]) -> bool:
    """`call` sits in the body of a `try`, inside `fn`, one of whose handlers
    refuses it."""
    node = call
    while node in parent and node is not fn:
        up = parent[node]
        if isinstance(up, (ast.Try, getattr(ast, "TryStar", ast.Try))):
            # `node` is the child of the `try` the walk came up through.
            in_body = any(node is stmt for stmt in up.body)
            if in_body and any(_refuses_it(h) for h in up.handlers):
                return True
        node = up
    return False


def walk_source(source: str, relpath: str, switches: Set[str]) -> List[Site]:
    """The shapes in one file. Pure, so it can be shown synthetic code."""
    tree = ast.parse(source)
    parent: Dict[ast.AST, ast.AST] = {}
    for p in ast.walk(tree):
        for c in ast.iter_child_nodes(p):
            parent[c] = p

    def enclosing(node, kinds):
        while node in parent:
            node = parent[node]
            if isinstance(node, kinds):
                return node
        return None

    def try_of(handler):
        return parent[handler]

    sites: List[Site] = []

    # 1. A log call saying "falling back to DuckDB".
    for call in (n for n in ast.walk(tree) if isinstance(n, ast.Call)):
        texts = [t for a in call.args for t in _string_parts(a)]
        if not any(FALLBACK_TEXT.search(t) for t in texts):
            continue
        fn = enclosing(call, (ast.FunctionDef, ast.AsyncFunctionDef))
        name = getattr(fn, "name", "<module>")
        handler = enclosing(call, (ast.ExceptHandler,))
        if handler is not None and (fn is None or enclosing(handler, (
                ast.FunctionDef, ast.AsyncFunctionDef)) is fn):
            ok = _decides(handler, try_of(handler))
        else:
            ok = False
        sites.append(Site("log", relpath, call.lineno, name, ok))

    # 2. An enabled()-and-available() gate: none may stand (S14-07). (Rule
    # 4, its address, went with `no_address` in S14-16.)
    for gate in (n for n in ast.walk(tree) if _is_gate(n)):
        fn = enclosing(gate, (ast.FunctionDef, ast.AsyncFunctionDef))
        sites.append(Site("gate", relpath, gate.lineno,
                          getattr(fn, "name", "<module>"), False))

    # 3. Every handler in a function that reaches another engine's read.
    if _in_handler_scope(relpath):
        modules, functions = _switch_names(tree, switches)
        for fn in _functions(tree):
            if not _reaches_another_engine(fn, modules, functions):
                continue
            for h, t in _handlers(fn):
                sites.append(Site("handler", relpath, h.lineno, fn.name,
                                  _decides(h, t)))

    # 5. A router with no gate left refuses every engine failure.
    if relpath.startswith("core/repositories/"):
        aliases, names = _engine_aliases(tree), _engine_names(tree)
        for fn in _functions(tree):
            name = getattr(fn, "name", "")
            refuses = any(isinstance(n, ast.Call) and _is_read_fallback_refuse(n)
                          for n in _own_nodes(fn))
            # A `_*_run` router, or anything that refuses at all: the
            # dashboard's Gold and Silver helpers are not called `_*_run`
            # (step 14, S14-06), and a refusal is a claim this rule checks.
            if not ((name.startswith("_") and name.endswith("_run")) or refuses):
                continue
            if any(_is_gate(n) for n in _own_nodes(fn)):
                continue                      # the old shape; rules 2-4
            calls = _engine_calls(fn, aliases, names)
            if not calls:
                continue
            reached = {_call_name(n) for n in _own_nodes(fn)
                       if isinstance(n, ast.Call)}
            if reached & TAIL_READS and not refuses:
                # A tail routed by a chain's own question — `_sms_run` — is a
                # shape of its own until its PR (`_goal_tables_run` was the
                # other until step 14's S14-07, and refuses now).
                continue
            ok = (all(_refused_where_it_fails(c, fn, parent) for c in calls)
                  and not reached & TAIL_READS and not reached & OLD_SHAPE)
            sites.append(Site("refuse", relpath, fn.lineno, name, ok))
    return sites


def walk_tree() -> List[Site]:
    switches = read_switch_modules()
    sites: List[Site] = []
    for top in WALKED:
        for path in sorted((REPO / top).rglob("*.py")):
            if "frontend" in path.parts or "node_modules" in path.parts:
                continue
            relpath = path.relative_to(REPO).as_posix()
            sites.extend(walk_source(path.read_text(encoding="utf-8"),
                                     relpath, switches))
    return sites


@pytest.fixture(scope="module")
def sites() -> List[Site]:
    return walk_tree()
